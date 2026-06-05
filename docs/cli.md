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
| `--branch <NAME>` | `$GITHUB_REF_NAME`, else `local` | Branch part of the manifest root key. |
| `--system <SYSTEM>` | detected | Nix system part of the root key (e.g. `x86_64-linux`). |
| `--upstream-cache-filter` | off | Skip paths signed by an upstream cache instead of caching them (saves quota for big closures). |
| `--upstream-cache-key-name <KEY_NAME>` | `cache.nixos.org-1` | Key names treated as upstream caches by the filter. Repeatable. |
| `--no-closure` | off | Cache built paths only, without their runtime closure. |
| `--db-path <PATH>` | `/nix/var/nix/db/db.sqlite` | Nix store database to read path metadata from. |

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
| `--dry-run` | off | Plan only; delete nothing. |
| `--grace <DAYS>` | `3` | Unreachable paths are kept this long. |
| `--push-ttl <DAYS>` | `14` | Recently pushed paths are kept, reachable or not. |
| `--root-ttl <DAYS>` | `14` | Roots (branch+system pins) expire after this. |
| `--touch-age <DAYS>` | `4` | Idle packs get an LRU touch after this. |

## Environment variables

| Variable | Used by | Description |
|---|---|---|
| `ACTIONS_RUNTIME_TOKEN` | serve, gc | GHA cache API token. Only visible to JS actions; the hestia action exports it. |
| `ACTIONS_RESULTS_URL` | serve, gc | GHA cache API base URL. Exported by the action. |
| `GITHUB_TOKEN` | gc | GitHub REST API token (`actions: write`) for listing/deleting cache entries. |
| `GITHUB_REPOSITORY` | gc | `owner/repo`, set automatically in workflows. |
| `GITHUB_API_URL` | gc | REST API base URL (override for GHES). |
| `GITHUB_REF_NAME` | serve | Default for `--branch`. |
| `GITHUB_RUN_ID` | serve | Roots written by the same workflow run merge by union (matrix legs); different runs replace each other's root. |
| `OUT_PATHS` | hook | Set by Nix when invoking the post-build-hook. |
