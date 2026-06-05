# hestia

> ⚠️ **Alpha software**: APIs, cache format, and behavior may change without
> notice. Not yet recommended for production CI.

Hestia is a Nix binary cache for GitHub Actions. It stores build results in
the GitHub Actions cache, so later runs download them instead of rebuilding.
There is nothing to set up: no accounts, no secrets, no server to run. Add
the action to your workflow and you have a binary cache.

How it differs from [magic-nix-cache]:

- Build results are packed into a few large cache entries instead of one per
  store path, which makes transfers a lot faster.
- Data is deduplicated in content-defined chunks, so a nixpkgs bump uploads
  only what changed rather than every rebuilt package.
- It makes far fewer GitHub API calls, so large builds don't run into
  `429 Too Many Requests`.
- A scheduled garbage-collection workflow keeps your repository inside
  GitHub's 10 GB cache quota by deleting paths no branch uses anymore.

[magic-nix-cache]: https://github.com/DeterminateSystems/magic-nix-cache

## Quick start

```yaml
# .github/workflows/ci.yml
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v6
      - uses: NixOS/nix-installer-action@main
      - uses: Mic92/hestia/action@main
      - run: nix build .#
```

Everything built in your workflow gets cached; later runs (and PRs) pull
from the cache instead of rebuilding.

Build jobs need no extra permissions: cache uploads authenticate with the
runner-injected `ACTIONS_RUNTIME_TOKEN`, which the `permissions:` block
does not scope.

You will also want a daily GC workflow on the default branch to stay within
the cache quota; copy [`.github/workflows/gc.yml`](.github/workflows/gc.yml)
for that (its REST cache deletes are what need `actions: write`).

See [Configuration](#configuration) for all action inputs.

## Comparison

|  | **hestia** | **magic-nix-cache** | **cachix** | **attic** |
|---|---|---|---|---|
| Status | alpha | maintained | commercial service | self-hosted |
| Storage | GHA cache (free, 10 GB/repo) | GHA cache (free, 10 GB/repo) | cachix.org | your S3/disk |
| Accounts / secrets needed | none | none | auth token | server + token |
| Infrastructure to run | none | none | none | server, database, storage |
| Uploads only what changed (dedup) | yes | no (whole store paths) | no | yes |
| Rate-limit errors on big builds | no | yes (`429`) | no | no |
| Garbage collection | automatic (scheduled workflow) | none (LRU eviction only) | retention rules | policies |
| Cache shared beyond CI | no (CI-only by design) | no | yes (any machine) | yes |
| Signing | not needed (`?trusted=true`, localhost) | not needed | yes | yes |
| Telemetry | none | reports usage to Determinate Systems (opt-out) | — | none |

If developer machines should hit the cache too, you want cachix or attic
instead; hestia only works inside CI.

## How it works

A small daemon (`hestia serve`) runs alongside your CI job:

```
nix build ──built paths──▶ hestia ──upload──▶ GitHub Actions cache
nix build ◀─cached paths── hestia ◀─download─ GitHub Actions cache
```

To Nix, the daemon looks like a regular binary cache: Nix asks it for paths
before building them and reports every path it does build. At the end of
the job, new build results and their runtime dependencies (the full closure,
nixpkgs packages included) are split into content-defined chunks, packed
into a few large blobs, and uploaded. Chunks that are already in the cache
are never uploaded again, and every download is hash-verified before Nix
gets to see it. The worst thing corrupt or evicted cache data can cause is a
rebuild, never wrong build inputs.

### Roots

Every job records the paths it pushed and the paths it downloaded under a
*root* named `<branch>-<system>`, e.g. `main-x86_64-linux`. The branch part
comes from `$GITHUB_REF_NAME` (override with `--branch`), the system part is
detected (override with `--system`). Anything reachable from a root survives
garbage collection; everything else is deleted once it falls out of the push
grace period.

Matrix jobs of one workflow run share their root: their closures are
unioned, however far apart the jobs finish. A new run replaces the root, so
old closures become collectable.

Pull requests get their own roots (`123/merge-x86_64-linux`), so a PR cannot
evict paths the default branch still needs. Roots that stop being updated
(merged PRs, deleted branches) expire after `--root-ttl` (14 days by
default) and their paths become collectable.

Roots are how hestia decides what is still alive. They are unrelated to
GitHub's own cache access scoping (who may read or write entries, see
[Security](#security)), which applies on top.

## Configuration

All inputs are optional; the defaults work for the quick start above.

| Input | Default | Description |
|---|---|---|
| `binary` | — | Path to a pre-built hestia binary. Takes precedence over `version`. |
| `version` | latest release | Release tag to download (e.g. `v0.1.0-alpha.10`). The download is verified against GitHub's build attestations. |
| `github-token` | `${{ github.token }}` | Token for the attestation API lookup. |
| `listen` | `127.0.0.1:37515` | Substituter listen address. |
| `socket` | `/tmp/hestia/hook.sock` | Post-build-hook unix socket path. |
| `drain-timeout` | `300` | Seconds the post-job step waits for the final upload. |
| `upstream-cache-filter` | `false` | Skip paths signed by an upstream cache instead of caching them (saves quota for big closures). |
| `upstream-cache-key-names` | `cache.nixos.org-1` | Space-separated key names treated as upstream caches by the filter. |
| `no-closure` | `false` | Cache built paths only, without their runtime closure. |

The GC workflow takes one input: `dry-run` (plan only, delete nothing); see
[`.github/workflows/gc.yml`](.github/workflows/gc.yml).

Running the `hestia` binary yourself instead of using the action? See the
[CLI reference](docs/cli.md).

## Security

### Why `?trusted=true` is safe here

Hestia serves unsigned narinfos, and the action configures the substituter
URL with `?trusted=true` so Nix accepts them. This does not weaken Nix's
trust model in CI: the substituter listens on `127.0.0.1` inside the job, and
everything it serves came either from the job's own builds or from cache
entries that only this repository's workflows could have written. If you
trust the runner to execute your build, there is nothing extra to trust
here.

### PR scope isolation (GitHub's model, not hestia's)

GitHub gives each cache entry an access scope: a PR job can read the default
branch's cache but can only write to its own PR scope, which is discarded
when the branch is deleted. In practice this means:

* A malicious PR cannot poison the cache used by `main` or by other PRs.
  Its writes land in its own scope and disappear with it.
* A malicious PR can read everything `main` cached (which is just
  already-public build outputs) and can fill its own scope with garbage,
  bounded by the 10 GB repository quota that GitHub evicts by LRU anyway.
* `pull_request_target` / fork PRs never get write tokens for the base
  scope; the standard GitHub Actions security guidance applies unchanged.

### What hestia itself enforces

Pack blobs are content-addressed (SHA-256-named, hash-verified on every
read), and NARs are verified against the manifest's NAR hash before being
served. Anything that doesn't check out is treated as a cache miss and gets
rebuilt.

## Limitations

* **10 GB per repository, shared.** The GHA cache quota covers all
  workflows of the repo (including `actions/cache` users). GitHub evicts
  least-recently-used entries under pressure and after 7 days idle. Hestia
  treats the cache as lossy: evicted paths are rebuilt and re-pushed.
* **Branch scoping.** PR builds read the default branch's cache but write
  only their own scope; GitHub enforces this server-side and it cannot be
  disabled. The shared cache therefore only grows when the default branch
  builds, and main does one full rebuild of changed paths after every merge.
  Run GC on the default branch only.
* **CI-only.** The cache API is unreachable from outside GitHub Actions;
  hestia cannot serve developer machines. Use cachix/attic for that.
* **Token lifetime.** The cache API token is a ~6 h JWT. Jobs that run
  longer than that lose the ability to upload near the end (you get a clear
  error, not corruption).
* **Eviction semantics.** A path can disappear between the narinfo lookup
  and the NAR fetch (eviction race). Nix falls back to building; with
  `fallback = true` (set by the action) this never fails a job.

## Development

```console
$ nix develop -c cargo test          # unit + integration tests (fake GHA backend)
$ nix develop -c cargo clippy --all-targets -- -D warnings
$ nix fmt                            # treefmt (rustfmt, nixfmt, taplo, ...)
$ nix flake check                    # everything CI runs: fmt, clippy, tests, build
$ nix build .#                       # the hestia binary (static musl on Linux)
```

## License

MIT
