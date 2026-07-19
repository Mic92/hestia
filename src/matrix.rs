//! `hestia matrix`: evaluate a flake once and emit a GitHub Actions build
//! matrix.
//!
//! Runs nix-eval-jobs over the flake, registers the resulting `.drv` paths
//! with the daemon, triggers a drain, and prints a matrix of not-yet-cached
//! jobs; build jobs then run `nix build <drvPath>^*` without re-evaluating.
//! The output shape matches nix-github-actions' `mkGithubMatrix`.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::process::{ExitCode, Stdio};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use crate::cli::MatrixArgs;
use crate::protocol::{self, Request};

/// Runner labels used when `--runner` does not override a system. Matches
/// nix-github-actions' `githubPlatforms` so migrations keep their runners.
const DEFAULT_RUNNERS: &[(&str, &str)] = &[
    ("x86_64-linux", "ubuntu-24.04"),
    ("aarch64-linux", "ubuntu-24.04-arm"),
    ("x86_64-darwin", "macos-13"),
    ("aarch64-darwin", "macos-14"),
];

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to run {command}: {source}")]
    Spawn {
        command: String,
        source: std::io::Error,
    },

    #[error("{command} exited with {status}")]
    EvalFailed {
        command: String,
        status: std::process::ExitStatus,
    },

    #[error("evaluation failed:\n{0}")]
    Eval(String),

    #[error("malformed nix-eval-jobs output line: {reason}: {line}")]
    Malformed { reason: String, line: String },

    #[error(
        "no runner mapping for system {0}; add --runner {0}=<label> or \
         --skip-unmapped-systems"
    )]
    UnmappedSystem(String),

    #[error("invalid --runner value {0:?}; expected <system>=<label>[,<label>...]")]
    RunnerSpec(String),

    #[error("group {group:?} mixes systems {a} and {b}; a matrix job runs on one runner")]
    MixedSystems { group: String, a: String, b: String },

    #[error("group {group:?} mixes runner labels {a:?} and {b:?}")]
    MixedRunners {
        group: String,
        a: Vec<String>,
        b: Vec<String>,
    },

    #[error("cannot write to $GITHUB_OUTPUT: {0}")]
    Output(std::io::Error),
}

/// One job as reported by nix-eval-jobs (the fields hestia cares about).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalJob {
    pub attr: String,
    pub drv_path: String,
    pub system: String,
    pub is_cached: bool,
    /// `meta.hestia.os`: per-job runner label override.
    pub runner_override: Option<Vec<String>>,
    /// `meta.hestia.group`: jobs sharing a group share one matrix row.
    pub group: Option<String>,
}

/// Parse nix-eval-jobs JSON-lines output. Evaluation errors (lines carrying
/// an `error` field) fail the whole run: a matrix silently missing checks
/// is worse than a failed eval job.
pub fn parse_jobs(output: &str) -> Result<Vec<EvalJob>, Error> {
    let mut jobs = Vec::new();
    let mut errors = Vec::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(|err| Error::Malformed {
            reason: err.to_string(),
            line: line.to_string(),
        })?;
        let attr = value["attr"].as_str().unwrap_or("<unknown>").to_string();
        if let Some(error) = value["error"].as_str() {
            errors.push(format!("{attr}: {error}"));
            continue;
        }
        let field = |name: &str| -> Result<String, Error> {
            value[name]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| Error::Malformed {
                    reason: format!("missing field {name:?}"),
                    line: line.to_string(),
                })
        };
        let hestia_meta = &value["meta"]["hestia"];
        jobs.push(EvalJob {
            attr,
            drv_path: field("drvPath")?,
            system: field("system")?,
            is_cached: value["isCached"].as_bool().unwrap_or(false),
            runner_override: parse_labels(&hestia_meta["os"]),
            group: hestia_meta["group"].as_str().map(str::to_string),
        });
    }
    if !errors.is_empty() {
        return Err(Error::Eval(errors.join("\n")));
    }
    Ok(jobs)
}

/// `meta.hestia.os`: a single label string or a list of labels.
fn parse_labels(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::String(label) => Some(vec![label.clone()]),
        Value::Array(labels) => Some(
            labels
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
        ),
        _ => None,
    }
}

/// system → runner labels, defaults overridden by `--runner` flags.
pub type RunnerMap = BTreeMap<String, Vec<String>>;

pub fn runner_map(overrides: &[String]) -> Result<RunnerMap, Error> {
    let mut map: RunnerMap = DEFAULT_RUNNERS
        .iter()
        .map(|(system, label)| ((*system).to_string(), vec![(*label).to_string()]))
        .collect();
    for spec in overrides {
        let (system, labels) = spec
            .split_once('=')
            .ok_or_else(|| Error::RunnerSpec(spec.clone()))?;
        let labels: Vec<String> = labels
            .split(',')
            .map(str::trim)
            .filter(|label| !label.is_empty())
            .map(str::to_string)
            .collect();
        if system.trim().is_empty() || labels.is_empty() {
            return Err(Error::RunnerSpec(spec.clone()));
        }
        map.insert(system.trim().to_string(), labels);
    }
    Ok(map)
}

/// One matrix row (one build job).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MatrixRow {
    pub name: String,
    pub system: String,
    pub os: Vec<String>,
    /// Full attribute path (first member for grouped rows), for the
    /// `nix build .#$attr` fallback mode.
    pub attr: String,
    /// Derivation path (first member for grouped rows).
    #[serde(rename = "drvPath")]
    pub drv_path: String,
    /// Space-separated `<drvPath>^*` list, ready for `nix build`.
    pub installables: String,
}

/// Turn eval jobs into matrix rows: dedup by drvPath, drop cached jobs,
/// resolve runners, and collapse `meta.hestia.group` members into one row.
pub fn build_rows(
    jobs: &[EvalJob],
    runners: &RunnerMap,
    skip_unmapped: bool,
    attr_prefix: &str,
) -> Result<Vec<MatrixRow>, Error> {
    let mut seen_drvs = std::collections::BTreeSet::new();
    let mut rows: Vec<MatrixRow> = Vec::new();
    // Group name → index into `rows`, so members merge into one row while
    // rows keep the eval order of their first member.
    let mut group_index: BTreeMap<String, usize> = BTreeMap::new();

    for job in jobs {
        if !seen_drvs.insert(job.drv_path.clone()) || job.is_cached {
            continue;
        }
        let os = match &job.runner_override {
            Some(labels) => labels.clone(),
            None => match runners.get(&job.system) {
                Some(labels) => labels.clone(),
                None if skip_unmapped => continue,
                None => return Err(Error::UnmappedSystem(job.system.clone())),
            },
        };
        let attr = prefixed(attr_prefix, &job.attr);
        let installable = format!("{}^*", job.drv_path);

        // A grouped job whose group already has a row merges into it.
        if let Some(group) = &job.group
            && let Some(&index) = group_index.get(group)
        {
            let row = &mut rows[index];
            if row.system != job.system {
                return Err(Error::MixedSystems {
                    group: group.clone(),
                    a: row.system.clone(),
                    b: job.system.clone(),
                });
            }
            if row.os != os {
                return Err(Error::MixedRunners {
                    group: group.clone(),
                    a: row.os.clone(),
                    b: os,
                });
            }
            row.installables.push(' ');
            row.installables.push_str(&installable);
            continue;
        }

        if let Some(group) = &job.group {
            group_index.insert(group.clone(), rows.len());
        }
        rows.push(MatrixRow {
            name: job.group.clone().unwrap_or_else(|| attr.clone()),
            system: job.system.clone(),
            os,
            attr,
            drv_path: job.drv_path.clone(),
            installables: installable,
        });
    }
    Ok(rows)
}

fn prefixed(prefix: &str, attr: &str) -> String {
    if prefix.is_empty() {
        attr.to_string()
    } else {
        format!("{}.{attr}", prefix.trim_end_matches('.'))
    }
}

/// The step outputs: `matrix`, `any-jobs`, `manifest-version`.
pub fn outputs(rows: &[MatrixRow], manifest_version: u64) -> Vec<(String, String)> {
    let matrix = serde_json::json!({ "include": rows });
    vec![
        ("matrix".to_string(), matrix.to_string()),
        ("any-jobs".to_string(), (!rows.is_empty()).to_string()),
        ("manifest-version".to_string(), manifest_version.to_string()),
    ]
}

/// Register the drv paths with the daemon and drain, returning the
/// committed manifest version. Never fatal: the matrix still works without
/// a reachable daemon (`nix build .#$attr` mode), so a warning beats
/// failing the eval job.
async fn register_and_drain(args: &MatrixArgs, drv_paths: Vec<String>) -> u64 {
    if drv_paths.is_empty() {
        return 0;
    }
    let count = drv_paths.len();
    let request = Request::Add { paths: drv_paths };
    if let Err(err) = protocol::roundtrip(&args.socket, &request).await {
        eprintln!(
            "hestia matrix: cannot register {count} drv path(s) with the daemon at {}: {err} \
             (matrix is still emitted; drv closures will not be cached)",
            args.socket.display()
        );
        return 0;
    }
    let drain = protocol::roundtrip(&args.socket, &Request::Drain);
    match tokio::time::timeout(Duration::from_secs(args.drain_timeout), drain).await {
        Ok(Ok(response)) => {
            let stats = response.stats.unwrap_or_default();
            eprintln!("hestia matrix: {}", crate::drain::summarize(&stats));
            stats.manifest_version
        }
        Ok(Err(err)) => {
            eprintln!("hestia matrix: drain failed: {err} (matrix is still emitted)");
            0
        }
        Err(_) => {
            eprintln!(
                "hestia matrix: daemon still draining after {}s; outcome unknown \
                 (matrix is still emitted)",
                args.drain_timeout
            );
            0
        }
    }
}

/// Run nix-eval-jobs and return its stdout (stderr is passed through).
async fn run_nix_eval_jobs(args: &MatrixArgs) -> Result<String, Error> {
    // Whitespace-split command string: covers "nix run ... --" style
    // wrappers and extra flags without a separate repeatable argument.
    let mut words = args.nix_eval_jobs.split_whitespace();
    let program = words.next().unwrap_or("nix-eval-jobs");
    let mut command = tokio::process::Command::new(program);
    command
        .args(words)
        .arg("--flake")
        .arg(&args.flake)
        .arg("--check-cache-status")
        .arg("--meta")
        // checks/hydraJobs are plain nested attrsets; without forced
        // recursion nix-eval-jobs finds no derivations in them.
        .arg("--force-recurse")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let output = command.output().await.map_err(|source| Error::Spawn {
        command: args.nix_eval_jobs.clone(),
        source,
    })?;
    if !output.status.success() {
        return Err(Error::EvalFailed {
            command: args.nix_eval_jobs.clone(),
            status: output.status,
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Print the outputs; under GitHub Actions also append them to
/// `$GITHUB_OUTPUT`.
fn emit_outputs(outputs: &[(String, String)]) -> Result<(), Error> {
    for (name, value) in outputs {
        println!("{name}={value}");
    }
    if let Ok(path) = std::env::var("GITHUB_OUTPUT")
        && !path.is_empty()
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .map_err(Error::Output)?;
        for (name, value) in outputs {
            writeln!(file, "{name}={value}").map_err(Error::Output)?;
        }
    }
    Ok(())
}

pub async fn run(args: &MatrixArgs) -> ExitCode {
    match run_inner(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("hestia matrix: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run_inner(args: &MatrixArgs) -> Result<(), Error> {
    let stdout = run_nix_eval_jobs(args).await?;
    let jobs = parse_jobs(&stdout)?;
    let runners = runner_map(&args.runners)?;
    let rows = build_rows(
        &jobs,
        &runners,
        args.skip_unmapped_systems,
        &args.attr_prefix,
    )?;

    // Register every distinct drv (cached outputs included: their drv may
    // still be missing from the cache) so build jobs can substitute the
    // drv closure.
    let mut drv_paths: Vec<String> = jobs.iter().map(|job| job.drv_path.clone()).collect();
    drv_paths.sort();
    drv_paths.dedup();
    let manifest_version = register_and_drain(args, drv_paths).await;

    eprintln!(
        "hestia matrix: {} job(s) to build ({} evaluated)",
        rows.len(),
        jobs.len()
    );
    emit_outputs(&outputs(&rows, manifest_version))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(attr: &str, system: &str) -> EvalJob {
        EvalJob {
            attr: attr.to_string(),
            drv_path: format!("/nix/store/{attr}-{system}.drv"),
            system: system.to_string(),
            is_cached: false,
            runner_override: None,
            group: None,
        }
    }

    fn default_runners() -> RunnerMap {
        runner_map(&[]).unwrap()
    }

    #[test]
    fn parses_jobs_meta_and_cache_status() {
        let output = concat!(
            r#"{"attr":"x86_64-linux.a","drvPath":"/nix/store/a.drv","system":"x86_64-linux","isCached":false,"meta":{"hestia":{"group":"small","os":"self-hosted"}}}"#,
            "\n",
            r#"{"attr":"x86_64-linux.b","drvPath":"/nix/store/b.drv","system":"x86_64-linux","isCached":true,"meta":{"hestia":{"os":["self-hosted","big"]}}}"#,
            "\n",
        );
        let jobs = parse_jobs(output).unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].group.as_deref(), Some("small"));
        assert_eq!(
            jobs[0].runner_override,
            Some(vec!["self-hosted".to_string()])
        );
        assert!(!jobs[0].is_cached);
        assert!(jobs[1].is_cached);
        assert_eq!(
            jobs[1].runner_override,
            Some(vec!["self-hosted".to_string(), "big".to_string()])
        );
    }

    #[test]
    fn eval_errors_fail_the_run_with_the_attr_and_message() {
        let output = concat!(
            r#"{"attr":"x86_64-linux.ok","drvPath":"/nix/store/ok.drv","system":"x86_64-linux"}"#,
            "\n",
            r#"{"attr":"x86_64-linux.broken","error":"assertion failed"}"#,
            "\n",
        );
        let err = parse_jobs(output).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("x86_64-linux.broken"), "{message}");
        assert!(message.contains("assertion failed"), "{message}");
    }

    #[test]
    fn malformed_lines_are_rejected() {
        assert!(matches!(
            parse_jobs("not json\n"),
            Err(Error::Malformed { .. })
        ));
        assert!(matches!(
            parse_jobs(r#"{"attr":"a","system":"x86_64-linux"}"#),
            Err(Error::Malformed { .. })
        ));
    }

    #[test]
    fn cached_jobs_and_duplicate_drvs_are_dropped() {
        let mut cached = job("cached", "x86_64-linux");
        cached.is_cached = true;
        let mut alias = job("a", "x86_64-linux");
        alias.attr = "alias-of-a".to_string();
        let jobs = vec![job("a", "x86_64-linux"), alias, cached];
        let rows = build_rows(&jobs, &default_runners(), false, "").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].attr, "a");
        assert_eq!(rows[0].os, vec!["ubuntu-24.04"]);
        assert_eq!(rows[0].installables, format!("{}^*", rows[0].drv_path));
    }

    #[test]
    fn runner_map_defaults_overrides_and_meta_os() {
        let runners = runner_map(&["x86_64-linux=self-hosted, big".to_string()]).unwrap();
        let mut meta_os = job("c", "x86_64-linux");
        meta_os.runner_override = Some(vec!["macos-15".to_string()]);
        let rows = build_rows(
            &[
                job("a", "x86_64-linux"),
                job("b", "aarch64-darwin"),
                meta_os,
            ],
            &runners,
            false,
            "",
        )
        .unwrap();
        assert_eq!(rows[0].os, vec!["self-hosted", "big"]);
        assert_eq!(rows[1].os, vec!["macos-14"]);
        assert_eq!(rows[2].os, vec!["macos-15"], "meta.hestia.os wins");

        assert!(matches!(
            runner_map(&["nonsense".to_string()]),
            Err(Error::RunnerSpec(_))
        ));
        assert!(matches!(
            runner_map(&["riscv64-linux=".to_string()]),
            Err(Error::RunnerSpec(_))
        ));
    }

    #[test]
    fn unmapped_systems_fail_or_are_skipped() {
        let jobs = vec![job("a", "riscv64-linux"), job("b", "x86_64-linux")];
        assert!(matches!(
            build_rows(&jobs, &default_runners(), false, ""),
            Err(Error::UnmappedSystem(system)) if system == "riscv64-linux"
        ));
        let rows = build_rows(&jobs, &default_runners(), true, "").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].attr, "b");
    }

    #[test]
    fn grouped_jobs_share_one_row() {
        let mut a = job("a", "x86_64-linux");
        let mut b = job("b", "x86_64-linux");
        a.group = Some("small".to_string());
        b.group = Some("small".to_string());
        let ungrouped = job("c", "x86_64-linux");
        let rows = build_rows(
            &[a.clone(), ungrouped, b.clone()],
            &default_runners(),
            false,
            "",
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "small");
        assert_eq!(rows[0].attr, "a");
        assert_eq!(rows[0].drv_path, a.drv_path);
        assert_eq!(
            rows[0].installables,
            format!("{}^* {}^*", a.drv_path, b.drv_path)
        );
        assert_eq!(rows[1].name, "c");
    }

    #[test]
    fn inconsistent_groups_are_rejected() {
        let mut a = job("a", "x86_64-linux");
        let mut b = job("b", "aarch64-darwin");
        a.group = Some("g".to_string());
        b.group = Some("g".to_string());
        assert!(matches!(
            build_rows(&[a.clone(), b], &default_runners(), false, ""),
            Err(Error::MixedSystems { group, .. }) if group == "g"
        ));

        let mut c = job("c", "x86_64-linux");
        c.group = Some("g".to_string());
        c.runner_override = Some(vec!["self-hosted".to_string()]);
        assert!(matches!(
            build_rows(&[a, c], &default_runners(), false, ""),
            Err(Error::MixedRunners { group, .. }) if group == "g"
        ));
    }

    #[test]
    fn attr_prefix_is_joined_with_a_dot() {
        assert_eq!(prefixed("checks", "a"), "checks.a");
        assert_eq!(prefixed("checks.", "a"), "checks.a");
        assert_eq!(prefixed("", "a"), "a");
    }

    #[test]
    fn outputs_are_matrix_any_jobs_and_manifest_version() {
        let rows = build_rows(&[job("a", "x86_64-linux")], &default_runners(), false, "").unwrap();
        let outputs = outputs(&rows, 7);
        assert_eq!(outputs[1], ("any-jobs".to_string(), "true".to_string()));
        assert_eq!(
            outputs[2],
            ("manifest-version".to_string(), "7".to_string())
        );
        let matrix: Value = serde_json::from_str(&outputs[0].1).unwrap();
        assert_eq!(matrix["include"][0]["name"], "a");
        assert_eq!(matrix["include"][0]["drvPath"], rows[0].drv_path);
        assert_eq!(matrix["include"][0]["os"][0], "ubuntu-24.04");

        let empty = super::outputs(&[], 0);
        assert_eq!(empty[1].1, "false");
        assert_eq!(empty[2].1, "0");
    }
}
