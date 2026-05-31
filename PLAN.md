# Hestia — Implementation Plan

Nix binary cache backed by the GitHub Actions cache (v2 API). Successor to the
abandoned magic-nix-cache (killed Feb 2025 with the cache API v1 shutdown).
Written in Rust, reusing harmonia crates.

Background design document: `~/.claude/outputs/gha-cache-handoff.md`
(architecture, GC design, storage math; its Go/niks3 code-reuse sections are
superseded by this plan).

## Summary

- `hestia serve` — one daemon per CI job:
  - **post-build-hook listener** (unix socket): receives locally-built store
    paths from Nix (`hestia hook` sends `$OUT_PATHS`).
  - **substituter** (HTTP, Nix binary cache protocol): serves previously
    cached paths back to Nix; narinfo hits double as liveness signal.
  - On drain (action post-step) or idle-exit: FastCDC-chunk new paths, pack
    new chunks, upload to GHA cache, update manifest, set root for this
    branch+system to *pushed ∪ accessed* paths (implicit pinning).
- `hestia gc` — scheduled workflow (default branch, cron):
  mark/sweep over roots, repack partially-dead packs (stability tiers),
  touch fully-live packs, delete garbage via GitHub REST API.
- `hestia-action` — **required** GitHub Action wrapper (see Auth below;
  shell steps cannot see the cache tokens).
- Only locally-built paths are stored. Anything signed by cache.nixos.org (or
  other configured upstreams) is filtered out.

Storage model on the GHA cache:

```
"pack-{sha256}"   chunk pack blobs (CAS, immutable, zstd frames concatenated)
"m#N"             manifest (CBOR + zstd, SaveMutable-versioned)
```

## Critical Constraints (research findings)

These shape the whole implementation; get them wrong and nothing works.

1. **Cache API tokens are invisible to shell steps.**
   `ACTIONS_RUNTIME_TOKEN` / `ACTIONS_RESULTS_URL` are only injected into the
   environment of *actions* (JS runtime), not `run:` steps. Hestia MUST ship
   a GitHub Action (JS shim or composite using `actions/github-script`) that
   captures these and exports them (`echo "ACTIONS_RUNTIME_TOKEN=..." >>
   $GITHUB_ENV`) before the daemon starts. Same trick as
   `crazy-max/ghaction-github-runtime`. → The action wrapper is Phase 0, not
   polish.

2. **`ACTIONS_RUNTIME_TOKEN` is a JWT valid ~6h**, scoped per job. Long jobs:
   get signed Azure URLs early (they outlive the token). The GC workflow gets
   its own token.

3. **Scope model**: a PR job has **read-write** access to its own ref scope
   and **read-only** access to the default-branch scope. So PR runs read
   main's manifest/packs but their writes land in a PR-scoped fork that dies
   with the branch. `hestia gc` only makes sense on the default branch.

4. **No signing needed.** Nix accepts unsigned paths from a substituter whose
   store URL carries `?trusted=true`:
   `substituters = http://127.0.0.1:37515?trusted=true`. The action writes
   nix.conf, so this is free. (Optional later: sign narinfos with
   `harmonia-utils-signature` + `build_narinfo()`; key from a repo secret.)

5. **Azure blob = plain REST, no SDK.** Upload/download URLs from the Twirp
   API are pre-signed SAS URLs. Single `PUT` with
   `x-ms-blob-type: BlockBlob` + `x-ms-version: 2020-04-08` handles blobs up
   to 5000 MiB; `GET` with `Range:` for chunk reads. The Rust Azure SDK is
   unnecessary (~50 LOC with reqwest). On 403 (URL expired): re-fetch URL via
   Twirp, retry (pattern from go-actions-cache `downloadV2`).

6. **Cache `version` field** is a namespace, not a format version. Pick one
   constant (e.g. `sha256("hestia-1")`) and never change it casually —
   changing it orphans all existing entries.

7. **Eviction is two-axis**: 7-day-idle LRU AND quota-pressure LRU
   (10 GB/repo, shared with all other workflows). Verified: downloads through
   the v2 path bump `last_accessed_at` (REST API shows it), so 1-byte Range
   reads work as LRU touches. Treat the cache as lossy; never serve partial
   data; heal manifest on detected eviction.

## Reuse from Harmonia

Harmonia (`~/git/harmonia`, nix-community/harmonia) is a production Nix binary
cache server. Crates pulled in as **git dependencies** (pin a rev; they are
not on crates.io):

| Hestia needs | Harmonia crate / item | Why it fits |
|---|---|---|
| Store path walk + NAR events | `harmonia-file-nar`: `NarDumper` (`dump(path)`) | Produces `Stream<NarEvent>`: file data as `Bytes` (small files buffered, large files mmap'd), directories/symlinks as events. Exactly the per-file access FastCDC needs. |
| NAR serialization (events → bytes) | `harmonia-file-nar`: `NarByteStream`, `NarWriter` | Converts `NarEvent` stream to NAR framing. Used twice: (a) write side to compute `nar_hash`, (b) read side to serve reassembled NARs. **Same code path → byte-identical NARs → hashes match by construction.** |
| File tree type for manifest | `harmonia-file-core`: `FileTree<C>` (generic over content) | `FileTree<ChunkList>` = manifest tree. Already has serde. |
| PathInfo via nix-daemon protocol | `harmonia-store-remote`: `DaemonClient`, `ConnectionPool`, `query_path_info` | No `nix path-info` subprocess; pooled unix-socket connections. |
| narinfo construction + formatting | `harmonia-store-nar-info`: `build_narinfo()`, text serialization | Handles URL/Compression/References/Sig lines; supports signing if we add it later. |
| Store path types | `harmonia-store-path`: `StorePath`, `StorePathHash` | Parsing/validation. |
| Signature parsing (upstream filter) | `harmonia-utils-signature`: `Signature::name()` | "Is this path signed by cache.nixos.org-1?" |
| Hashes / base32 | `harmonia-utils-hash`, `harmonia-utils-base-encoding` | nar_hash formatting for narinfo/URLs. |
| Unix socket plumbing | `harmonia-utils-io`: `unix_socket` | Hook listener, socket activation. |

**Key insight**: harmonia's `NarEvent` stream IS the abstraction layer between
"contents from disk" and "contents from chunks". Write side consumes dumper
events; read side synthesizes events from manifest + fetched chunks and feeds
them to the same `NarByteStream`. No harmonia changes needed.

**Not reused** (deliberately):

- `harmonia-cache` (the actix-web server crate): coupled to local-store
  serving, TLS, HTML templates, prometheus. Hestia's substituter is ~3 routes
  over a manifest — write it fresh (~300 LOC), reference harmonia-cache's
  handlers for protocol details.
- `harmonia-store-db` (direct SQLite): daemon protocol is the safer default
  (works with sandboxing/multi-user); revisit only if daemon round-trips show
  up in profiles.

## New Dependencies

| Purpose | Crate | Notes |
|---|---|---|
| FastCDC chunking | `fastcdc` 4.x | v2020 variant, streaming API |
| Manifest serialization | `ciborium` (CBOR) + `serde` | NOT protobuf: `FileTree` already has serde; CBOR is compact, schema-evolves with `#[serde(default)]`, no build.rs/codegen |
| HTTP client (Twirp, REST, Azure) | `reqwest` (rustls) | |
| HTTP server (substituter) | `axum` | Lighter than actix for a 3-route localhost server; tokio-native |
| zstd | `zstd` | already in harmonia's dep tree |
| CLI | `clap` | |
| Async runtime | `tokio` | matches harmonia |

Dropped from earlier draft: `prost`/`prost-build` (CBOR instead), `rusqlite`
(see hook section — queue is pointless on ephemeral runners), Azure SDK.

## Architecture

```
hestia serve                       (one process per CI job)
├── unix socket  ← hestia hook ($OUT_PATHS from Nix post-build-hook)
├── HTTP :37515  ← Nix substituter (?trusted=true)
│     /nix-cache-info
│     /{hash}.narinfo    → manifest lookup → build_narinfo() → record access
│     /nar/{hash}.nar    → chunks → NarEvents → NarByteStream → stream
└── on drain (action post-step) or idle-exit:
      paths → DaemonClient.query_path_info → filter upstream-signed
      → NarDumper events → FastCDC chunks (skip known) + nar_hash
      → pack new chunks → Twirp reserve → Azure PUT → finalize
      → manifest merge: paths, chunks, pack, root = pushed ∪ accessed
      → SaveMutable "m#N+1" (retry/re-merge on conflict)

hestia gc                          (cron workflow, default branch)
├── REST list "pack-*" → last_accessed_at; reconcile evicted packs (heal)
├── mark: paths reachable from roots (references walk, upstream edges skipped)
├── sweep: roots > RootTTL; paths unreachable > PathGrace and > PushTTL
├── plan: repack (liveness < 0.5 or volatile count > 4, stability tiers)
│         touch (fully live, last_accessed > 4d → 1-byte Range read)
├── execute: Range-copy live chunks (verify sha256) → new packs → upload
│            → commit manifest → REST DELETE replaced/garbage packs
└── crash-safe ordering: old packs stay referenced until manifest commit
```

Liveness rule (why old nixpkgs generations get cleaned up):

```
live(path) := reachable from any root via references
           OR now - last_pushed < PushTTL (default 14d)
```

Roots are replaced per (branch, system) on every run → paths only in the old
closure become unreachable → dead after grace → their chunks get dropped at
next repack. Full reasoning, stability tiers, and edge cases: handoff doc.

Manifest schema (serde structs, CBOR-encoded, zstd-compressed):

```rust
struct Manifest {
    paths: BTreeMap<StorePathHash, PathEntry>,
    chunks: BTreeMap<ChunkHash, ChunkLocation>,
    packs: Vec<PackRef>,
    roots: BTreeMap<String, Root>,        // "main-x86_64-linux"
}

struct PathEntry {
    nar_hash: [u8; 32],
    nar_size: u64,
    references: Vec<StorePathHash>,
    ca: Option<String>,
    deriver: Option<String>,
    tree: FileTree<ChunkList>,            // harmonia-file-core, C = ChunkList
    last_reachable: u64,                  // GC mark clock
    last_pushed: u64,                     // push clock (also bumped on dedup-skip)
}

struct ChunkList(Vec<ChunkHash>);          // ordered chunks of one file

struct ChunkLocation {
    pack: PackHash,
    offset: u64,
    compressed_size: u32,
    uncompressed_size: u32,
    repacks_survived: u32,                 // stability tier promotion
}

struct PackRef { hash: PackHash, size: u64, created: u64, tier: u8 }

struct Root { paths: BTreeSet<StorePathHash>, updated: u64 }
```

## Hook: keep it minimal

`hestia hook` reads `$OUT_PATHS`, writes one JSON line to the unix socket,
exits 0 always (a failing post-build-hook fails the build — never do that).

No SQLite queue (earlier draft had one, copied from niks3): on ephemeral
runners the disk dies with the job, so a persistent queue protects nothing.
The daemon buffers paths in memory and uploads on drain. If the daemon
crashes, the paths are rebuilt next run (cache miss) and re-pushed —
self-correcting. Self-hosted persistent runners can re-evaluate this later.

## GitHub Action (`hestia-action`)

Required for token capture (Constraint 1). Composite action:

```yaml
runs:
  using: composite
  steps:
    - uses: actions/github-script@v7     # capture runtime tokens
      with:
        script: |
          core.exportVariable('ACTIONS_RUNTIME_TOKEN', process.env.ACTIONS_RUNTIME_TOKEN)
          core.exportVariable('ACTIONS_RESULTS_URL', process.env.ACTIONS_RESULTS_URL)
    - run: |                             # install hestia, write nix.conf, start daemon
        hestia serve --daemonize
        echo "extra-substituters = http://127.0.0.1:37515?trusted=true" | sudo tee -a /etc/nix/nix.conf
        echo "post-build-hook = $(which hestia-hook)" | sudo tee -a /etc/nix/nix.conf
      shell: bash
post:                                    # drain: upload + manifest commit
  - run: hestia drain --timeout 300
    shell: bash
```

(Exact mechanism TBD in Phase 0 — `actions/github-script` exposes the runtime
token to later steps; verify `ACTIONS_RESULTS_URL` is also present.)

## Implementation Phases

Each phase ends with passing tests (`cargo test`, `cargo clippy`, flake-fmt)
and is independently committable. Red-green TDD.

### Phase 0: Scaffolding + token access (the existential risk first)

**Status: done** (except the CI milestone, which can only run once the repo
is pushed to GitHub — the `token-probe` job in `.github/workflows/ci.yml`
validates it on the first push). Deviations recorded under Open Questions
(5–7).

- [x] cargo init, flake.nix (rust toolchain, treefmt), CI workflow
- [x] harmonia git deps resolve and build (all ten crates, pinned to rev
      `3ef11b5b`; none had to be dropped)
- [x] clap skeleton: `hestia serve|hook|drain|gc` (stubs exit 0 with a
      "not implemented yet" notice; flag parsing unit-tested)
- [x] **hestia-action prototype**: composite action captures
      `ACTIONS_RUNTIME_TOKEN` + `ACTIONS_RESULTS_URL` via
      `actions/github-script` and exports them to later shell steps
- [x] bonus: `packages.default` (buildRustPackage with
      `cargoLock.allowBuiltinFetchGit`) so `nix build`/`nix run` work
- [ ] **Milestone: a CI run lists its own cache entries via Twirp using
      captured tokens.** Pending first push to GitHub; `token-probe` asserts
      token visibility and endpoint reachability. If this fails, the whole
      project needs rethinking — do it first.

### Phase 1: GHA cache client

**Status: done** (code + fake-backend tests green locally; the real-API
suite runs in the CI `token-probe` job and validates the milestone on the
first push). Deviations recorded under Open Questions (8–9).

- [x] `gha/twirp.rs`: CreateCacheEntry, FinalizeCacheEntryUpload,
      GetCacheEntryDownloadURL (request shapes ported from
      go-actions-cache `cache_v2.go`; `already_exists` surfaces as
      `Reservation::AlreadyExists`, not an error)
- [x] `gha/blob.rs`: Azure PUT (BlockBlob, single-shot), GET with Range,
      403 → caller-provided async refresh callback → retry once
- [x] `gha/rest.rs`: list (prefix, pagination), usage, delete
      (`GITHUB_TOKEN`; 404 on delete maps to empty result for idempotence)
- [x] `gha/savemutable.rs`: prefix load → highest N, reserve/retry loop,
      re-merge on conflict, stale-reservation skip (crashed writers)
- [x] `tests/support/fake_gha.rs`: behavioral fake (axum + tempdir blobs)
      with eviction + URL-expiry injection endpoints
- [x] Tests: 13 scenarios against fake-gha locally; same scenarios against
      the real API in `tests/gha_real.rs` (`#[ignore]` locally, run in CI
      with `cargo test --test gha_real -- --ignored`)
- [ ] **Milestone: round-trip a blob through the real GHA cache from CI;
      Range-read a slice of it; delete it via REST.** Pending first push
      (test uses 256 KB; bump to 100 MB once CI proves the path works).

### Phase 2: Manifest + chunker

- `manifest.rs`: serde structs above, CBOR+zstd encode/decode, merge rules
  (paths/chunks union, root union-or-replace by timestamp, pack dedup),
  reachability walk, liveness predicate
- `chunker.rs`: fastcdc v2020 (16/64/256 KB), per-file chunking from
  `NarEvent::File` data, pack assembly (chunks ordered by path+offset),
  offset bookkeeping
- Tests: chunk determinism, merge commutativity (A⊕B = B⊕A for concurrent
  runs), reachability with upstream holes, CBOR round-trip + forward-compat
  (unknown fields ignored)
- **Milestone: chunk a real store path; reconstruct every file byte-identical
  from the pack buffer; nar_hash from event replay matches
  `nix path-info`.**

### Phase 3: Write pipeline + hook

- `hook.rs`: `hestia hook` (client) + unix socket listener in serve
- `upstream.rs`: signature-name filtering (default trusted:
  `cache.nixos.org-1`, configurable)
- `pipeline.rs`: paths → `query_path_info` (harmonia-store-remote) → filter
  → `dump()` events → tee(chunker, nar_hash) → pack → upload → manifest
  commit with root update
- `serve.rs`: daemon lifecycle (socket, drain command, idle-exit, SIGTERM)
- Tests: end-to-end with real store paths + fake-gha backend; daemon
  drain under concurrent hook sends
- **Milestone: `nix build` with post-build-hook on a runner → paths appear
  as pack + manifest entries in the repo's GHA cache.**

### Phase 4: Substituter

- `substituter.rs`: axum app
  - `/nix-cache-info` (Priority 30, WantMassQuery 1)
  - `/{hash}.narinfo`: manifest lookup → `build_narinfo()` (Compression:
    none) → record access
  - `/nar/{hash}.nar`: chunk plan → batched Range fetches per pack
    (parallel, prefetch on narinfo hit) → synthesize `NarEvent`s →
    `NarByteStream` → stream response
  - any chunk 404 → whole NAR 404 (nix falls through; never partial data)
- Tests: narinfo text vs `nix path-info --json`; NAR round-trip hash check;
  scratch-store substitution: `nix copy --from http://localhost:37515
  --store /tmp/scratch` into an empty chroot store, compare contents
- **Milestone: second CI run substitutes from cache instead of rebuilding;
  ephemeral runner store gets populated from packs. Hestia's own CI starts
  dogfooding hestia.**

### Phase 5: GC

- `gc.rs`: plan (REST reconcile, mark, sweep, schedule repack/touch) +
  execute (verified Range-copy, upload, manifest commit, REST delete)
- Stability tiers (repacks_survived → volatile/stable packs)
- Tests: simulated histories (nixpkgs bumps, branch deletion, quota
  eviction mid-run), plan idempotence, crash-ordering (kill between any two
  steps → no data loss), GC vs concurrent push (SaveMutable conflict →
  re-plan)
- **Milestone: simulated 30-day history (daily pushes, weekly bumps)
  converges to ≈ live-set storage; no pack older than TouchAge.**

### Phase 6: Hardening + release

- hestia-action: proper post-step drain, `gc` reusable workflow example
- Failure-mode tests: token expiry mid-upload, Azure 403/timeouts, quota
  exhaustion, manifest corruption (truncated upload)
- README: setup guide, comparison with magic-nix-cache/cachix/attic,
  configuration reference, security notes (trusted=true rationale,
  PR scope isolation)
- Decide: nix-community adoption, binary cache of hestia itself for
  bootstrap (`nix run github:.../hestia`)

## Testing Strategy

Five layers, ordered by feedback speed. Core tension: the GHA cache API only
exists inside real GitHub Actions jobs (token capture), but most logic must
be testable locally.

### 1. Unit tests (local, milliseconds)

Pure logic, no I/O:

- Chunker determinism: same input → same chunk hashes (FastCDC params pinned)
- Manifest merge: commutativity (A⊕B = B⊕A), idempotence — proptest
- Reachability walk with upstream holes
- CBOR round-trip + forward-compat (unknown fields ignored)
- narinfo text vs golden files

### 2. Fake GHA backend (local, the workhorse)

`tests/support/fake_gha.rs` — a **behavioral fake**, not request stubs: HTTP
server storing blobs on disk, implementing the 3 Twirp RPCs, Azure
PUT/Range-GET, and the 3 REST endpoints. Stateful: `already_exists`, ref
scoping, signed-URL expiry (403 after N seconds), injectable LRU eviction
(`DELETE /test/evict/{key}`).

Why a fake instead of wiremock stubs: pipeline and GC tests need stateful
sequences (reserve → upload → finalize → list → evict → heal), and eviction
scenarios must be injectable mid-test.

Fidelity: real API responses captured once from actual GHA runs, kept as
golden fixtures so the fake doesn't drift.

### 3. Real store path tests (local + CI, needs /nix/store)

Realistic inputs, never mocked:

- Chunk a real store path → reconstruct every file byte-identical
- NAR event replay → nar_hash matches `nix path-info --json`
- `query_path_info` against the real nix-daemon
- Scratch-store substitution: push to fake-gha, then
  `nix copy --from http://localhost:37515 --store /tmp/scratch` into an
  empty chroot store — nix itself is the correctness oracle

### 4. Real GHA API tests (CI only)

Marked `#[ignore]` locally, run in the GitHub Actions workflow with captured
tokens:

- Token capture works at all (Phase 0 existential check)
- Blob round-trip, Range read, REST list/delete
- Catches API drift the fake can't

### 5. End-to-end

- **GC simulation**: 30-day compressed history against fake-gha — daily
  pushes, weekly nixpkgs bumps, mid-run evictions, branch deletion. Assert:
  storage converges to live-set size, no pack older than TouchAge, no live
  path lost.
- **Crash safety**: kill the pipeline between every pair of steps →
  invariants hold (old packs referenced until manifest commit, orphans
  cleaned next run).
- **Dogfooding**: from Phase 4 on, hestia's own CI uses hestia. Cache
  misses, corruption, and eviction handling show up as slow or failing
  builds immediately.

No NixOS VM test: it cannot have real tokens (so it would test the fake
backend — same coverage as layer 2/3, slower), and the "clean store"
property comes free from chroot stores. The real environment test is
dogfooding.

### What never gets mocked

Nix store, nix-daemon, NAR format, chunking, compression. Only the GHA HTTP
API gets faked, and only because GitHub gives no other choice locally.

## Open Questions

1. **Multi-system runs** (e.g. matrix x86_64-linux + aarch64-darwin in one
   workflow): separate roots per system work, but should packs be
   system-tagged so GC can repack them separately? Probably yes — chunk
   locality per system improves Range batching. Decide in Phase 2.
2. **Manifest size at scale**: 531 paths ≈ 40 KB compressed. A monorepo with
   10k local paths ≈ ~800 KB. Fine. But the reachability walk and merge are
   O(paths) per drain — measure in Phase 3, consider incremental merge if
   slow.
3. **Idle-exit vs post-step drain**: post-step is reliable on GitHub-hosted
   runners; idle-exit is the fallback for setups that can't run post steps
   (act, some self-hosted). Ship both; default to post-step.
4. **harmonia API stability**: git-pinned rev; harmonia refactors freely.
   Budget for occasional `cargo update` breakage. If it hurts, ask harmonia
   to publish the leaf crates (file-nar, file-core, store-path,
   utils-hash/signature/base-encoding) to crates.io.
5. **Composite actions cannot declare `post:`** (Phase 0 finding). The
   action.yml sketch above showing a top-level `post:` is invalid for
   composite actions; only JS (`using: nodeXX`) actions support post hooks.
   Workaround shipped in Phase 0: a nested node20 action (`action/post`)
   whose `post:` entry point will run `hestia drain`. Caveat: the nested
   `uses: ./action/post` path resolves relative to the consumer's workspace,
   which works for repo-local use (`./action`) but breaks when the action is
   consumed from another repo. Before publishing (Phase 6), either convert
   the wrapper to a single JS action (no `actions/github-script` dependency,
   native `post:`) or use `${{ github.action_path }}`-based resolution.
6. **reqwest 0.13 renamed the TLS feature**: `rustls-tls` → `rustls`
   (Phase 0 finding; the dependency table said "reqwest (rustls)").
   Also note: no certificate root store feature is enabled yet; Phase 1
   must pick `rustls-platform-verifier` (current default pulled in) or
   `webpki-roots` explicitly when real HTTPS calls are added.
7. **Nix package uses `cargoLock.allowBuiltinFetchGit`** instead of
   per-crate `outputHashes`. Works, but means the package build shells out
   to builtins.fetchGit at eval time (needs network for uncached evals).
   If that becomes a problem (e.g. pure-eval contexts), switch to explicit
   `outputHashes` — every harmonia crate in Cargo.lock needs an entry, all
   sharing the same hash.
8. **Upload-URL refresh is unsolved** (Phase 1 finding). Download URLs can
   be refreshed by calling GetCacheEntryDownloadURL again, but there is no
   Twirp call that re-issues an upload URL for an already-reserved key
   (CreateCacheEntry returns already_exists). If an upload outlives its SAS
   URL (~long uploads on slow links), the entry is stuck: it can neither be
   uploaded nor re-reserved. Mitigation for now: upload promptly after
   reserving, keep packs well under a size where this matters. Revisit in
   Phase 3 (pipeline) — possibly split giant packs.
9. **Twirp lookup misses are ambiguous** (Phase 1 finding): the service can
   signal "no entry" either as HTTP 200 + `ok=false` or as a Twirp
   `not_found` error depending on path. The client treats both as a miss;
   the fake uses `ok=false` (matching go-actions-cache's expectation).
   Verify against the real service output once CI runs.

## Mistakes Fixed from Earlier Draft

| Was | Now | Why |
|---|---|---|
| protobuf/prost manifest | CBOR via serde | `FileTree` already serde; no codegen; simpler |
| "ContentSource seam" adaptation of NAR code | none needed | `NarEvent` stream already is the seam |
| reuse `harmonia-cache` crate + `zstd_body.rs` | fresh 3-route axum app, Compression: none | localhost serving needs no response compression; harmonia-cache too coupled |
| SQLite hook queue | in-memory buffer | ephemeral runner disk dies with the job anyway |
| Azure SDK | raw REST (PUT BlockBlob / Range GET) | Rust Azure SDK unstable; 2 endpoints needed |
| narinfo signing required | `?trusted=true` store URL | action controls nix.conf; signing optional later |
| Action wrapper in "polish" phase | Phase 0 | shell steps can't see cache tokens — existential dependency |
| niks3-hook protocol compat | own minimal protocol | compat has zero value; protocol is internal |
