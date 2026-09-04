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
| `HESTIA_OCI` | serve, gc | `<registry>/<repository>`: store in an OCI registry instead of the Actions cache, see [OCI registries](#oci-registries). |
| `HESTIA_OCI_USER`, `HESTIA_OCI_PASSWORD` | serve, gc | Registry credentials. On ghcr.io `GITHUB_TOKEN` is used when unset. Without any, access is anonymous and read-only. |
| `HESTIA_S3` | serve, gc | `s3://<bucket>/<prefix>`: store in an S3-compatible bucket. Credentials from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, region from `AWS_REGION` (default `us-east-1`). Without credentials access is anonymous and read-only. Takes precedence over `HESTIA_OCI`. `https://<host>/<prefix>` reads the same layout over plain HTTP, see [Public buckets and CDNs](#public-buckets-and-cdns). |
| `HESTIA_S3_ENDPOINT` | serve, gc | Endpoint URL for non-AWS stores, addressed path-style. Without it `https://s3.<region>.amazonaws.com`, virtual-hosted style. |
| `HESTIA_TRUST` | serve, gc | Head policy, one `<root glob \| @gc> <cosign \| gh> <args…>` per line: a head counts only if a row for its root (first matching glob, `@gc` for GC records) verifies its bundle with `cosign verify-blob-attestation <args>` or `gh attestation verify <args>`. Unset accepts everything. |
| `HESTIA_SIGN` | serve, gc | `cosign attest-blob` arguments for published heads (empty: keyless). Unset publishes unsigned. |
| `HESTIA_LISTEN` | prefetch | Address exported by the action for the running Hestia server. |
| `OUT_PATHS` | hook | Set by Nix when invoking the post-build-hook. |

## OCI registries

With `HESTIA_OCI=<registry>/<repository>` (action input `oci`) packs and
segments are blobs and heads are tags in that repository. Any registry
that speaks the OCI distribution API can hold the store. They differ in
how GC can delete:

| Registry | Delete | Notes |
|---|---|---|
| ghcr.io | GitHub packages API by version id, `GITHUB_TOKEN` with `packages: write` | The OCI delete is refused, so gc keeps a version ledger under the `x-ledger` tag; content manifests stay untagged (GHCR never reaps them). No pull limits from Actions runners. Public packages substitute anonymously. |
| distribution, Harbor, Quay, GitLab, zot, Gitea, ACR, Artifact Registry | `DELETE /v2/…/manifests/<digest>` | Every object is tagged with its key (`pack-…`, `seg-…`, `tree-…` next to the heads), so retention rules for untagged manifests and `garbage-collect --delete-untagged` are safe to run and gc enumerates objects through `tags/list`. The registry's own garbage collection reclaims blob storage after gc deleted a manifest (self-hosted `distribution` needs `delete.enabled` and a `garbage-collect` cron). Keep tag-based retention rules away from those prefixes. |
| AWS ECR | not supported yet (needs `BatchDeleteImage`) | Push and pull work. |
| Docker Hub | tags only, untagged manifests are reaped by Hub | Push and pull work, but the pull rate limit (100 to 200 requests per 6 h per IP, shared by all GitHub-hosted runners) makes it unsuitable as a CI cache. |


## Public buckets and CDNs

`HESTIA_S3=https://cache.example.org/hestia` reads a bucket through anything
that serves its objects over HTTP: the bucket's own public endpoint, a website
endpoint, or a CDN in front of it. The URL is the bucket root plus an optional
key prefix, and the store is read-only: `gc` and pushing need `s3://`.

Only `GetObject` has to be public. Heads are not discovered by listing but
read from `<prefix>/index`, a newline-separated list of head names that every
writer rewrites when it publishes or deletes a head (a GET for the current
`ETag`, then a conditional PUT, retried if another writer won). Anonymous
listing would let anyone enumerate and page through the whole bucket, which is
both a disclosure and a bill, so hestia never asks for it.

An AWS bucket policy is therefore just:

```json
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
  "Action":"s3:GetObject","Resource":"arn:aws:s3:::my-cache/hestia/*"}]}
```

which also covers stores whose public endpoint cannot list at all, such as
Cloudflare R2 and Backblaze B2.

Writes carry the `Cache-Control` a CDN in front of the bucket should honour:
content-addressed objects are `immutable` for a year, `index` and the heads
`max-age=30`, since a stale copy of those means readers miss recently pushed
paths until it expires.
