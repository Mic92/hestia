# hestia

> ⚠️ **Alpha software**: APIs, cache format, and behavior may change without
> notice. Not yet recommended for production CI.

Speed up your Nix builds on GitHub Actions — for free, with zero setup.

Hestia caches everything your CI builds in the **GitHub Actions cache**, so
later runs download results instead of rebuilding them. No accounts, no
secrets, no servers to run: add one action to your workflow and you have a
binary cache.

Compared to [magic-nix-cache], hestia is built for speed and efficiency:

- **Faster cache transfers.** Build results are bundled into a few large
  uploads/downloads instead of one cache entry per store path.
- **Only changes are uploaded.** Data is deduplicated in small chunks, so a
  nixpkgs bump re-uploads only what actually changed — not every rebuilt
  package.
- **No rate-limit errors.** Far fewer GitHub API calls means no more
  `429 Too Many Requests` failures on large builds.
- **The cache cleans itself.** A scheduled garbage-collection workflow keeps
  your repository inside GitHub's 10 GB cache quota, keeping what your
  branches still use and dropping the rest.

[magic-nix-cache]: https://github.com/DeterminateSystems/magic-nix-cache

## Quick start

```yaml
# .github/workflows/ci.yml
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      actions: write          # GHA cache writes
    steps:
      - uses: actions/checkout@v6
      - uses: NixOS/nix-installer-action@main
      - uses: Mic92/hestia/action@main
        with:
          version: v0.1.0-alpha.1
          sha256: <sha256 of the release binary>
      - run: nix build .#
```

Plus a daily GC workflow on the default branch — copy
[`.github/workflows/gc.yml`](.github/workflows/gc.yml).

See [`action/README.md`](action/README.md) for all action inputs.

That's it. Everything built in your workflow is cached; later runs (and PRs)
pull from the cache instead of rebuilding.

## Comparison

|  | **hestia** | **magic-nix-cache** | **cachix** | **attic** |
|---|---|---|---|---|
| Status | alpha | maintained | commercial service | self-hosted |
| Storage | GHA cache (free, 10 GB/repo) | GHA cache (free, 10 GB/repo) | cachix.org | your S3/disk |
| Accounts / secrets needed | none | none | auth token | server + token |
| Infrastructure to run | none | none | none | server, database, storage |
| Uploads only what changed (dedup) | ✅ | ❌ (whole store paths) | ❌ | ✅ |
| Rate-limit errors on big builds | no | yes (`429`) | no | no |
| Garbage collection | automatic (scheduled workflow) | none (LRU eviction only) | retention rules | policies |
| Cache shared beyond CI | ❌ (CI-only by design) | ❌ | ✅ (any machine) | ✅ |
| Signing | not needed (`?trusted=true`, localhost) | not needed | ✅ | ✅ |
| Telemetry | none | reports usage to Determinate Systems (opt-out) | — | none |

Use cachix or attic when developer machines should hit the cache too.
Use hestia when you want CI caching with zero accounts, zero secrets, and
zero infrastructure.

## How it works

A small daemon (`hestia serve`) runs alongside your CI job:

```
nix build ──built paths──▶ hestia ──upload──▶ GitHub Actions cache
nix build ◀─cached paths── hestia ◀─download─ GitHub Actions cache
```

Nix reports every path it builds to the daemon, and asks it for paths before
building them — to Nix it looks like a regular binary cache. At the end of
the job, new build results are split into content-defined chunks, packed
into a few large blobs, and uploaded. Chunks already in the cache are never
uploaded again, and every download is hash-verified before Nix gets to see
it. Corrupt or evicted cache data simply means a rebuild — never wrong build
inputs.

### Roots: what keeps cache data alive

Every job records the paths it pushed and the paths it downloaded under a
*root* named `<branch>-<system>`, e.g. `main-x86_64-linux`. The branch part
comes from `$GITHUB_REF_NAME` (override with `--branch`), the system part is
detected (override with `--system`). Anything reachable from a root survives
garbage collection; everything else is deleted once it falls out of the push
grace period.

Pull requests get their own roots (`123/merge-x86_64-linux`), so a PR cannot
evict paths the default branch still needs. Roots that stop being updated —
merged PRs, deleted branches — expire after `--root-ttl` (14 days by
default) and their paths become collectable.

This is hestia's liveness tracking inside the manifest. It is separate from
GitHub's own cache access scoping (who may read/write entries, see
[Security](#security)); both apply.

The full architecture and design rationale live in [PLAN.md](PLAN.md).

## Configuration reference

The action covers the common case. Direct CLI use:

### `hestia serve` — per-job daemon

| Flag | Default | Description |
|---|---|---|
| `--socket <PATH>` | `/tmp/hestia/hook.sock` | Unix socket for the post-build-hook listener. |
| `--listen <ADDR>` | `127.0.0.1:37515` | Substituter HTTP address. |
| `--idle-exit <SECONDS>` | — | Drain and exit after this much inactivity (fallback for setups without post steps). |
| `--branch <NAME>` | `$GITHUB_REF_NAME`, else `local` | Branch part of the manifest root key. |
| `--system <SYSTEM>` | detected | Nix system part of the root key (e.g. `x86_64-linux`). |
| `--upstream-key <KEY_NAME>` | `cache.nixos.org-1` | Trusted upstream signature names; paths signed by these are never uploaded. Repeatable. |
| `--db-path <PATH>` | `/nix/var/nix/db/db.sqlite` | Nix store database to read path metadata from. |

### `hestia hook` — post-build-hook client

| Flag | Default | Description |
|---|---|---|
| `--socket <PATH>` | `/tmp/hestia/hook.sock` | Daemon socket. |
| `[PATH]...` | `$OUT_PATHS` | Store paths to register. |

Always exits 0 (a failing post-build-hook would fail the build).

### `hestia drain` — upload + commit

| Flag | Default | Description |
|---|---|---|
| `--socket <PATH>` | `/tmp/hestia/hook.sock` | Daemon socket. |
| `--timeout <SECONDS>` | `300` | Maximum time to wait for the upload. |

### `hestia gc` — garbage collection (cron, default branch)

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | off | Plan only; delete nothing. |
| `--grace <DAYS>` | `3` | Unreachable paths are kept this long. |
| `--push-ttl <DAYS>` | `14` | Recently pushed paths are kept, reachable or not. |
| `--root-ttl <DAYS>` | `14` | Roots (branch+system pins) expire after this. |
| `--touch-age <DAYS>` | `4` | Idle packs get an LRU touch after this. |

### Environment variables

| Variable | Used by | Description |
|---|---|---|
| `ACTIONS_RUNTIME_TOKEN` | serve, gc | GHA cache API token. Only visible to JS actions; the hestia action exports it. |
| `ACTIONS_RESULTS_URL` | serve, gc | GHA cache API base URL. Exported by the action. |
| `GITHUB_TOKEN` | gc | GitHub REST API token (`actions: write`) for listing/deleting cache entries. |
| `GITHUB_REPOSITORY` | gc | `owner/repo`, set automatically in workflows. |
| `GITHUB_API_URL` | gc | REST API base URL (override for GHES). |
| `GITHUB_REF_NAME` | serve | Default for `--branch`. |
| `OUT_PATHS` | hook | Set by Nix when invoking the post-build-hook. |

## Security

**Why `?trusted=true` is safe here.** Hestia serves *unsigned* narinfos, and
the action configures the substituter URL with `?trusted=true` so Nix accepts
them. This does not weaken Nix's trust model in CI: the substituter listens
on `127.0.0.1` inside the job, and everything it serves came either from the
job's own builds or from cache entries that **only this repository's
workflows** could have written. Trusting it is exactly as safe as trusting
the runner that is already executing your build.

**PR scope isolation (GitHub's model, not hestia's).** GitHub gives each
cache access scope: a PR job can *read* the default branch's cache but can
only *write* to its own PR scope, which is discarded when the branch is
deleted. Consequences:

* A malicious PR **cannot** poison the cache used by `main` or by other PRs.
  Its writes land in its own scope and die with it.
* A malicious PR **can** read everything `main` cached (already-public build
  outputs) and can fill its own scope with garbage — bounded by the 10 GB
  repo quota that GitHub evicts by LRU anyway.
* `pull_request_target` / fork PRs never get write tokens for the base
  scope; the standard GitHub Actions security guidance applies unchanged.

**What hestia itself enforces.** Pack blobs are content-addressed
(SHA-256-named, hash-verified on every read), NARs are verified against the
manifest's NAR hash before being served, and corrupt or evicted data turns
into a cache miss (rebuild) — never into wrong build inputs.

## Limitations

* **10 GB per repository, shared.** The GHA cache quota covers *all*
  workflows of the repo (including `actions/cache` users). GitHub evicts
  least-recently-used entries under pressure and after 7 days idle. Hestia
  treats the cache as lossy: evicted paths are rebuilt and re-pushed.
* **Branch scoping.** PR builds read the default branch's cache but write
  only their own scope. The shared cache only grows when the default branch
  builds. Run GC on the default branch only.
* **CI-only.** The cache API is unreachable from outside GitHub Actions;
  hestia cannot serve developer machines. Use cachix/attic for that.
* **Token lifetime.** The cache API token is a ~6 h JWT. Jobs longer than
  that lose upload ability near the end (clear error, no corruption).
* **Eviction semantics.** A path can disappear between the narinfo lookup
  and the NAR fetch (eviction race). Nix falls back to building; with
  `fallback = true` (set by the action) this never fails a job.

## Development

```console
$ nix develop -c cargo test          # unit + integration tests (fake GHA backend)
$ nix develop -c cargo clippy --all-targets -- -D warnings
$ nix fmt                            # treefmt (rustfmt, nixfmt, prettier, ...)
$ nix flake check                    # everything CI runs: fmt, clippy, tests, build
$ nix build .#                       # the hestia binary
$ nix build .#static                 # static (musl) release binary
```

Architecture, design decisions, and the full implementation plan live in
[PLAN.md](PLAN.md).

## License

MIT
