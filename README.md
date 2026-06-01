# hestia

> ⚠️ **Alpha software**: APIs, cache format, and behavior may change without
> notice. Not yet recommended for production CI.

A Nix binary cache backed by the **GitHub Actions cache** (v2 API).

Hestia is the successor to [magic-nix-cache], which died in February 2025
when GitHub shut down the v1 cache API. It gives every GitHub repository a
free, zero-configuration Nix binary cache: store paths built in one CI run
are served back to later runs instead of being rebuilt.

[magic-nix-cache]: https://github.com/DeterminateSystems/magic-nix-cache

## How it works

One `hestia serve` daemon runs per CI job. Nix's `post-build-hook` reports
every locally-built store path to the daemon over a unix socket; at the end
of the job the daemon **chunks** those paths (FastCDC content-defined
chunking over the NAR stream), packs new chunks into content-addressed
**pack** blobs, uploads them to the GHA cache over its Twirp/Azure API, and
commits a **manifest** (CBOR+zstd, versioned via monotonically numbered
cache keys) that maps store paths → file trees → chunks → packs. The same
daemon doubles as a localhost **substituter** speaking the Nix binary cache
protocol: narinfo lookups come straight from the manifest, NARs are
reassembled from Range-read chunks and hash-verified before a single byte is
served. Chunk-level deduplication means a nixpkgs bump re-uploads only what
actually changed, and a mark/sweep garbage collector (run as a cron workflow)
keeps the whole thing inside GitHub's 10 GB cache quota.

```
nix build ──post-build-hook──▶ hestia serve ──drain──▶ GHA cache
nix build ◀──substituter───── hestia serve ◀──chunks── (packs + manifest)
```

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

## Comparison

|  | **hestia** | **magic-nix-cache** | **cachix** | **attic** |
|---|---|---|---|---|
| Status | alpha | **dead** (cache API v1 shutdown, Feb 2025) | commercial service | self-hosted |
| Storage | GHA cache (free, 10 GB/repo) | GHA cache v1 | cachix.org | your S3/disk |
| Accounts / secrets needed | none | none | auth token | server + token |
| Infrastructure to run | none | none | none | server, database, storage |
| Chunk-level dedup | ✅ (FastCDC) | ❌ (whole NARs) | ❌ | ✅ |
| Garbage collection | mark/sweep cron workflow | LRU only | retention rules | policies |
| Cache shared beyond CI | ❌ (CI-only by design) | ❌ | ✅ (any machine) | ✅ |
| Signing | not needed (`?trusted=true`, localhost) | not needed | ✅ | ✅ |

Use cachix or attic when developer machines should hit the cache too.
Use hestia when you want CI caching with zero accounts, zero secrets, and
zero infrastructure.

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
$ nix build .#                       # the hestia binary
$ nix build .#static                 # static (musl) release binary
```

Architecture, design decisions, and the full implementation plan live in
[PLAN.md](PLAN.md).

## License

MIT
