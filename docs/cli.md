# CLI reference

The action takes care of all of this; these flags are only relevant if you
run the `hestia` binary yourself (e.g. token-capture-only mode, self-hosted
setups, or hacking on hestia).

## `hestia serve` — per-job daemon

| Flag | Default | Description |
|---|---|---|
| `--socket <PATH>` | `/tmp/hestia/hook.sock` | Unix socket for the post-build-hook listener. |
| `--listen <ADDR>` | `127.0.0.1:37515` | Substituter HTTP address. |
| `--idle-exit <SECONDS>` | — | Drain and exit after this much inactivity (fallback for setups without post steps). |
| `--branch <NAME>` | `$GITHUB_REF_NAME`, else `local` | Branch part of the root key. |
| `--system <SYSTEM>` | detected | Nix system part of the root key (e.g. `x86_64-linux`). |
| `--serve-branch <BRANCH>` | `main` | Also serve what these branches' roots hold (repeatable). |
| `--wait-head <NAME>` | — | Wait up to 60s at startup until this head is listed (matrix build jobs pass the eval job's `head` output). |
| `--upstream-cache-filter` | off | Skip paths signed by an upstream cache instead of caching them (saves quota for big closures). |
| `--upstream-cache-key-name <KEY_NAME>` | `cache.nixos.org-1` | Key names treated as upstream caches by the filter. Repeatable. |
| `--filter-drv-closures` | off | Apply the upstream filter to registered derivation closures. Requires `--upstream-cache-filter`; use `hestia prefetch` to retain bulk closure fetching. |
| `--read-only` | off | Serve the cache for substitution but never write to it (no uploads, nothing published). |
| `--no-closure` | off | Cache built paths only, without their runtime closure. |
| `--db-path <PATH>` | `/nix/var/nix/db/db.sqlite` | Nix store database to read path metadata from. |

## `hestia prefetch` — bulk drv closure fetch

Prepares references omitted by `--filter-drv-closures` through the runner's
configured Nix substituters, then imports the Hestia-backed closure in one
request. It accepts the `<drvPath>^*` values emitted by `hestia matrix` and
requires Nix 2.15 or newer.

| Flag / argument | Default | Description |
|---|---|---|
| `--listen <ADDR>` | `$HESTIA_LISTEN`, else `127.0.0.1:37515` | Running Hestia server address. |
| `<STORE_PATH>...` | — | Store paths or `<drvPath>^*` installables to prefetch. |

## `hestia hook` — post-build-hook client

| Flag | Default | Description |
|---|---|---|
| `--socket <PATH>` | `/tmp/hestia/hook.sock` | Daemon socket. |
| `[PATH]...` | `$OUT_PATHS` | Store paths to register. |

Always exits 0 (a failing post-build-hook would fail the build).

## `hestia drain` — upload + commit

| Flag | Default | Description |
|---|---|---|
| `--socket <PATH>` | `/tmp/hestia/hook.sock` | Daemon socket. |
| `--timeout <SECONDS>` | `300` | Maximum time to wait for the upload. |

## `hestia gc` — garbage collection (cron, default branch)

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | off | Plan only; upload and delete nothing. |
| `--root-ttl <DAYS>` | `14` | Roots without a drain for this long are dropped. |
| `--touch-age <DAYS>` | `4` | Idle live packs get an LRU touch after this. |

## Environment variables

| Variable | Used by | Description |
|---|---|---|
| `ACTIONS_RUNTIME_TOKEN` | serve, gc | GHA cache API token. Only visible to JS actions; the hestia action exports it. |
| `ACTIONS_RESULTS_URL` | serve, gc | GHA cache API base URL. Exported by the action. |
| `GITHUB_TOKEN` | serve, gc | GitHub REST API token for listing cache entries (`actions: read`), gc also deletes (`actions: write`). |
| `GITHUB_REPOSITORY` | gc | `owner/repo`, set automatically in workflows. |
| `GITHUB_API_URL` | gc | REST API base URL (override for GHES). |
| `GITHUB_REF_NAME` | serve | Default for `--branch`. |
| `GITHUB_RUN_ID` | serve | Roots written by the same workflow run merge by union (matrix legs); different runs replace each other's root. |
| `HESTIA_OCI` | serve, gc | `<registry>/<repository>` (e.g. `ghcr.io/owner/repo/hestia`): store in that OCI registry instead of the Actions cache. Credentials from `HESTIA_OCI_USER`/`HESTIA_OCI_PASSWORD`, else `GITHUB_TOKEN` on ghcr.io, else anonymous (read-only). gc cannot delete there yet. |
| `HESTIA_LISTEN` | prefetch | Address exported by the action for the running Hestia server. |
| `OUT_PATHS` | hook | Set by Nix when invoking the post-build-hook. |
