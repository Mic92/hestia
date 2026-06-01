# Hestia — Implementation Plan

Nix binary cache backed by the GitHub Actions cache (v2 API). Alternative to
magic-nix-cache, adding chunk-level deduplication, packed storage (faster
transfers, far fewer GHA API calls) and mark/sweep garbage collection.
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
| PathInfo from the store database | `harmonia-store-db`: `StoreDb`, `query_path_info` | Direct SQLite reads, same as harmonia-cache in production. Works without a daemon (single-user installs, scratch stores in tests). *(Originally planned as harmonia-store-remote / daemon protocol; revised in Phase 3, see Decision 11.)* |
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
- `harmonia-store-remote` (nix-daemon protocol): originally chosen as "the
  safer default", dropped in Phase 3 in favor of `harmonia-store-db` — a
  daemon only exists on multi-user installs, while the database exists
  wherever paths were built (Decision 11).

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
      paths → store-db query_path_info → filter upstream-signed
      → NarDumper events → FastCDC chunks (skip known) + nar_hash
        recomputed from the chunked representation (integrity gate)
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

**Status: done.** The CI milestone was validated after the first push
(Mic92/hestia, PR #1): the `token-probe` job captures the runtime tokens
and reaches the cache API on a real runner. Deviations recorded under
Decisions (5–7).

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
- [x] **Milestone: a CI run lists its own cache entries via Twirp using
      captured tokens.** Validated on a real runner (PR #1 `token-probe`
      job): tokens are visible to shell steps after the action exports
      them, and the results endpoint answers.

### Phase 1: GHA cache client

**Status: done.** The real-API suite runs in the CI `token-probe` job and
passes on real runners; it caught one fake/real divergence (download
lookups only consult restore keys — Decision 9). Deviations recorded under
Decisions (8–9).

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
- [x] **Milestone: round-trip a blob through the real GHA cache from CI;
      Range-read a slice of it; delete it via REST.** Passing in CI
      (`tests/gha_real.rs` in the `token-probe` job). The test uses 256 KB;
      bumping to 100 MB is still worthwhile once uploads get exercised by
      dogfooding.

### Phase 2: Manifest + chunker

**Status: done.** Milestone verified locally: bash-5.3 store path (43 files)
reconstructed byte-identical from a pack; NAR hash matches both
`nix-store --dump` and `nix path-info --json`. Schema deviations recorded
under Decisions (10).

- [x] `manifest.rs`: serde structs, CBOR+zstd encode/decode (Hash32 as CBOR
      byte strings, PathHash as base32 strings), merge rules (paths/chunks
      union with deterministic winners, root union-or-replace by timestamp
      with 10-min concurrency window, pack dedup by hash), reachability walk
      (skips upstream holes, tolerates cycles), liveness predicate
- [x] `chunker.rs`: fastcdc v2020 (16/64/256 KB), per-file chunking from
      `NarEvent::File` data, pack assembly (chunks ordered by path+offset,
      individually zstd-compressed, Range-extractable), offset bookkeeping,
      hash-verified extraction
- [x] NAR integration: `chunk_path()` (NarDumper → chunks + FileTree),
      `nar_hash_and_size()` (NarByteStream → sha256, same code path that
      will serve NARs in Phase 4)
- [x] Tests: chunk determinism, CDC shift-resistance, merge laws via
      proptest (commutativity, idempotence, identity, no-path-loss),
      reachability with upstream holes, CBOR round-trip + forward-compat
      (unknown fields ignored), pack offset tiling
- [x] **Milestone: chunk a real store path; reconstruct every file
      byte-identical from the pack buffer; nar_hash from event replay
      matches `nix path-info`.**

### Phase 3: Write pipeline + hook

**Status: done.** The on-runner milestone is validated by the `action-test`
CI job (build through the post-build-hook → drain → pack + manifest in the
real GHA cache). Deviations recorded under Decisions (11–12).

- [x] `protocol.rs` + `hook.rs`: JSON-lines socket protocol; `hestia hook`
      reads `$OUT_PATHS` (or explicit args) and always exits 0 — verified
      by spawning the real binary against unreachable sockets, error
      responses, and servers that hang up
- [x] `upstream.rs`: signature-name filtering (default trusted:
      `cache.nixos.org-1`, repeatable `--upstream-key` to override)
- [x] `pathinfo.rs`: store-database reads via harmonia-store-db (NOT the
      daemon protocol — see Decision 11)
- [x] `pipeline.rs`: batch path-info query → filter (invalid / upstream /
      already-stored with last_pushed bump) → chunk → NAR-hash verification
      from the chunked representation (integrity gate, see Decision 12) → pack → upload (already_exists = skip) → SaveMutable commit with
      re-merge; root = pushed ∪ accessed; `AccessLog` is the Phase 4
      substituter integration point
- [x] `serve.rs`: daemon lifecycle (hook socket, serialized drains with
      failed-drain retry, idle-exit, SIGTERM/SIGINT → final drain);
      `drain.rs`: post-step client with one-line summary, fails loudly
      (unlike hook)
- [x] Tests: hermetic scratch stores (`nix-store --store 'local?store=…'
      --add`, fabricated upstream signatures via `nix store sign`,
      references via `builtins.toFile`) + fake-gha; pipeline e2e incl.
      dedup re-runs and cross-path chunk sharing; daemon drain under
      concurrent hook sends; SaveMutable conflict between two concurrent
      pipelines
- [x] **Milestone: `nix build` with post-build-hook on a runner → paths
      appear as pack + manifest entries in the repo's GHA cache.**
      Validated by the `action-test` CI job.

### Phase 4: Substituter

**Status: done.** Substitution out of the real GHA cache is validated by
the `action-test` CI job (`nix copy` from the hestia substituter into a
fresh store on a real runner); full-CI dogfooding stays gated until after
the merge to main. Deviations and findings recorded under Decisions
(13–16).

- [x] `substituter.rs`: axum app
  - [x] `/nix-cache-info` (Priority 30, WantMassQuery 1, scratch-store
        aware StoreDir)
  - [x] `/{hash}.narinfo`: manifest lookup → `build_narinfo()` +
        `format_narinfo_txt()` from harmonia (Compression: none, unsigned)
        → record access → spawn background chunk prefetch
  - [x] `/nar/{narhash}.nar`: chunk plan grouped by pack → coalesced Range
        fetches (adjacent chunks share a request), packs in parallel,
        signed pack URLs cached with TTL + refreshed on 403 → synthesize
        NAR from tree + chunks → hash-verify → respond (see Open
        Question 14: buffered, not streamed)
  - [x] any chunk failure (eviction, 404, hash mismatch) → whole NAR 404
        (nix falls through; never partial data)
- [x] Wired into `hestia serve`: shared `ManifestStore` (loaded at startup,
      refreshed after every drain) + shared `AccessLog` (narinfo hits join
      the next drain's root); substituter traffic resets the idle-exit timer
- [x] Tests: narinfo fields vs `nix path-info --json`; NAR round-trip hash
      check; scratch-store substitution via real `nix copy` and via
      `nix-store --realise` with a `?trusted=true` substituter (byte-for-
      byte tree comparison); pack eviction → clean 404 + failed `nix copy`
      leaves no partial path; access recording → root update; prefetch → no
      duplicate pack reads; URL expiry mid-serving → transparent refresh;
      manifest refresh without restart
- [x] **Milestone: substitution from the real GHA cache on a real runner.**
      The `action-test` CI job builds a run-unique derivation through the
      post-build-hook, drains, and `nix copy`s the path back out of the
      cache into a fresh store. Full dogfooding (the regular `build` CI
      job runs `nix build .#` through hestia, so cache breakage breaks CI)
      remains gated behind the `HESTIA_DOGFOOD` repository variables until
      the first release exists — the daemon must be bootstrapped from a
      released binary, since the build job cannot use the package it is
      about to build.

### Phase 5: GC

**Status: done.** Milestone verified: the 30-day simulation (daily pushes,
weekly nixpkgs bumps, one branch deletion, one quota eviction) converges to
live-set storage within 20%, no referenced pack idles past TouchAge, and
every live path stays readable through the substituter. Deviations and
findings recorded under Decisions (17–20).

- [x] `gc.rs`: plan (REST reconcile, mark, sweep, schedule repack/touch) +
      execute (verified Range-copy, upload, manifest commit, REST delete),
      split into individually resumable steps (repack → touch → commit →
      delete packs → delete orphans → cleanup old manifest versions)
- [x] Stability tiers (repacks_survived → volatile/stable packs); CAS
      no-op trap guarded (fully live packs are touched, never repacked)
- [x] GC policy defaults from the design: PathGrace 72h, PushTTL 14d,
      RootTTL 14d, TouchAge 4d, MinLiveness 0.5, MaxVolatilePacks 4,
      StableThreshold 2 (CLI flags: --grace, --push-ttl, --root-ttl,
      --touch-age, --dry-run)
- [x] Tests: mark/sweep unit tests (root expiry, grace, PushTTL fallback),
      repack with verification, stability-tier promotion, touch/LRU reset,
      eviction healing + re-push, plan idempotence, crash-ordering (stop
      after every execute step → no live path references a deleted pack →
      next run converges), GC vs concurrent push (SaveMutable conflict →
      re-plan inside the merge closure)
- [x] **Milestone: simulated 30-day history (daily pushes, weekly bumps)
      converges to ≈ live-set storage; no pack older than TouchAge.**

### Phase 6: Hardening + release

**Status: done** (except the items listed under "Pending" below, which
need a merge to main / a human decision). Findings recorded under
Decisions (21–27).

- [x] Failure-mode tests against fake-gha (extended error injection):
      token expiry mid-upload (clear error, no partial commit), quota
      exhaustion (graceful failure, orphaned packs cleaned by next GC),
      Azure connection drops (transparent retry, clean 404 when
      persistent), manifest corruption (daemon starts with empty manifest,
      never crashes CI; GC refuses to act), concurrent serve daemons
      (manifests merge, nothing lost)
- [x] hestia-action rework: single node24 JS action with native `post:`
      drain; sha256-verified release install or local-binary input;
      nix.conf wiring (?trusted=true&priority=30, post-build-hook shim,
      fallback); daemon start + /nix-cache-info readiness poll;
      `action/README.md`
- [x] CI `action-test` job: full end-to-end loop on a real runner
      (install → build → hook → drain → substitute back out of the cache)
- [x] `.github/workflows/gc.yml`: daily cron GC on the default branch
      (doubles as the reference example for other repositories)
- [x] `README.md`: what/why, architecture, quick start, configuration
      reference, comparison table, security notes, limitations
- [x] Release plumbing: `.github/workflows/release.yml` (tag push → static
      musl binaries for x86_64-linux + aarch64-linux → GitHub release),
      Cargo.toml metadata, version 0.1.0-alpha.1
- [x] Real-API validation: token capture, blob round-trip/Range/delete,
      restore-key semantics — all green in CI on the real cache service

**Pending (needs merge to main / human decision):**

- Create the first release: tag `v0.1.0-alpha.1` on main, verify the
  release workflow, then update the sha256 in the README/action examples
- Enable dogfooding (after the release): set the repository variables
  `HESTIA_DOGFOOD=true`, `HESTIA_DOGFOOD_VERSION=<tag>`,
  `HESTIA_DOGFOOD_SHA256=<sha256 of hestia-x86_64-linux>` so the regular
  `build` job runs its `nix build .#` through hestia
- nix-community migration (repository transfer + harmonia crates on
  crates.io would remove the git-dependency pin)

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
- **Dogfooding**: hestia's own CI uses hestia. The regular `build` job in
  ci.yml wraps `nix build .#` with the hestia action (gated on the
  `HESTIA_DOGFOOD` repository variables; the step is skipped instantly
  while they are unset). Once enabled, cache misses, corruption, and
  eviction handling show up as slow or failing regular CI — the strongest
  possible signal.

No NixOS VM test: it cannot have real tokens (so it would test the fake
backend — same coverage as layer 2/3, slower), and the "clean store"
property comes free from chroot stores. The real environment test is
dogfooding.

### What never gets mocked

Nix store, nix-daemon, NAR format, chunking, compression. Only the GHA HTTP
API gets faked, and only because GitHub gives no other choice locally.

## Open Questions

Genuinely unresolved items. Everything that got decided or answered during
implementation lives under [Decisions](#decisions) below; numbering is
shared between the two sections (an item keeps its number when it moves).

1. **Multi-system runs** (e.g. matrix x86_64-linux + aarch64-darwin in one
   workflow): separate roots per system work (and are covered by tests),
   but packs are not system-tagged, so GC repacks mix systems and Range
   batching loses some locality. Tag packs by system once a real
   multi-system workload shows the cost matters.
2. **Manifest size at scale**: 531 paths ≈ 40 KB compressed. A monorepo with
   10k local paths ≈ ~800 KB. Fine. But the reachability walk and merge are
   O(paths) per drain — not yet measured under a large real workload;
   consider incremental merge if dogfooding shows it is slow.
4. **harmonia API stability**: git-pinned rev; harmonia refactors freely.
   Budget for occasional `cargo update` breakage. If it hurts, ask harmonia
   to publish the leaf crates (file-nar, file-core, store-path,
   utils-hash/signature/base-encoding) to crates.io.
8. **Upload-URL refresh is unsolved** (Phase 1 finding). Download URLs can
   be refreshed by calling GetCacheEntryDownloadURL again, but there is no
   Twirp call that re-issues an upload URL for an already-reserved key
   (CreateCacheEntry returns already_exists). If an upload outlives its SAS
   URL (~long uploads on slow links), the entry is stuck: it can neither be
   uploaded nor re-reserved. Mitigation for now: upload promptly after
   reserving, keep packs well under a size where this matters. Revisit
   when packs grow — possibly split giant packs.

## Decisions

Resolved questions and findings, in the order they came up. Numbering
continues from / interleaves with the Open Questions section above.

3. **Idle-exit vs post-step drain**: post-step is reliable on GitHub-hosted
   runners; idle-exit is the fallback for setups that can't run post steps
   (act, some self-hosted). Ship both; default to post-step.
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
   *Resolved in Phase 6*: the wrapper is now a single node24 JS action with
   a native `post:` hook (Decision 25).
6. **reqwest 0.13 renamed the TLS feature**: `rustls-tls` → `rustls`
   (Phase 0 finding; the dependency table said "reqwest (rustls)").
   Resolved in Phase 1: reqwest 0.13 dropped the `webpki-roots` option
   entirely, so `rustls-platform-verifier` (system CA certs) is the only
   choice. Consequence: constructing *any* reqwest Client requires CA
   certs, even for plain-HTTP localhost use. Real runners have them; the
   Nix build sandbox gets them via `SSL_CERT_FILE` in nix/package.nix.
7. **Nix package uses `cargoLock.allowBuiltinFetchGit`** instead of
   per-crate `outputHashes`. Works, but means the package build shells out
   to builtins.fetchGit at eval time (needs network for uncached evals).
   If that becomes a problem (e.g. pure-eval contexts), switch to explicit
   `outputHashes` — every harmonia crate in Cargo.lock needs an entry, all
   sharing the same hash.
9. **Twirp lookup misses are ambiguous** (Phase 1 finding): the service can
   signal "no entry" either as HTTP 200 + `ok=false` or as a Twirp
   `not_found` error depending on path. The client treats both as a miss;
   the fake uses `ok=false` (matching go-actions-cache's expectation).
   *Verified against the real service in Phase 6* — with one extra finding:
   GetCacheEntryDownloadURL ignores the `key` field for matching and only
   prefix-matches `restore_keys` (Decision 24).
10. **Manifest schema deviations from the PLAN sketch** (Phase 2):
    `ChunkList` is a struct with a named `chunks` field (not a tuple
    newtype) because harmonia's `Regular<C>` flattens its contents with
    serde, which requires map-shaped serialization. `packs` is a
    `BTreeMap<PackHash, PackInfo>` instead of `Vec<PackRef>` so dedup-by-
    hash is the natural merge operation. Also: harmonia's `Hash::FromStr`
    is commented out upstream; hash parsing goes through
    `harmonia_utils_hash::fmt::Any<Sha256>`.
11. **PathInfo comes from direct store-database reads, not the daemon
    protocol** (Phase 3 decision, revising the original plan). The plan
    table picked harmonia-store-remote ("daemon protocol is the safer
    default"), but that claim did not survive contact: a daemon only
    exists on multi-user installs, while the SQLite database exists
    wherever paths were built — and a post-build-hook by definition runs
    on the machine that built the paths. harmonia-cache reads the database
    directly in production, so the access path is battle-tested. Direct
    reads also make tests hermetic: a scratch store created with
    `nix-store --store 'local?store=…' --add` is queryable without
    spawning a daemon, lets tests fabricate upstream signatures
    (`nix store sign` with a key named `cache.nixos.org-1`), and controls
    references via `builtins.toFile` interpolation. A `nix path-info`
    subprocess fallback was also considered and rejected (subprocess
    parsing for no environment gain). Tests needing a store database probe
    for it at runtime and skip with a notice when missing.
12. **chunk_path walks the path twice** (Phase 2) — resolved in Phase 3.
    The pipeline now computes the NAR hash from the *chunked
    representation* (`nar_hash_from_chunks`: synthesized events →
    NarWriter → SHA-256) instead of a second disk walk. Besides removing
    the double walk, this is a strictly stronger check: equality with the
    store database's nar_hash proves the data hestia uploads can be served
    back byte-identically (a chunker bug now fails the drain instead of
    surfacing as hash mismatches on some future substitution). It is also
    the exact code path the Phase 4 substituter will use, so write and
    read side cannot drift apart.
13. **The manifest stores full store paths, not just hashes** (Phase 4
    schema change). narinfo responses need full basenames
    (`<hash>-<name>`) for the StorePath and References lines, so
    `PathEntry` records `store_path: StorePath` and keeps references and
    deriver as `StorePath` values instead of `PathHash`/`String`. The
    manifest key stays the hash; the reachability walk derives keys from
    the stored paths. Breaking schema change, irrelevant in practice: no
    manifest exists outside tests before the first push.
14. **NAR responses are buffered and verified, not streamed** (Phase 4).
    The plan sketched streaming through `NarByteStream`; the
    implementation assembles the full NAR in memory, checks its SHA-256
    against the manifest's nar_hash, and only then responds. Reason: the
    "never partial/corrupt data" rule. With a streamed response, a chunk
    failure or hash mismatch discovered mid-body can only abort the
    connection after bytes (with a 200 status) have already been sent.
    All chunks must be fetched before serving anyway, so buffering costs
    one extra copy of nar_size bytes, briefly. Revisit if NARs get large
    enough for this to matter (CI artifacts are typically well under 1 GB).
15. **Nix store-URL details for substitution** (Phase 4 test findings).
    (a) Nix's http binary cache client checks the cache's advertised
    StoreDir against its *own* store prefix, which defaults to
    /nix/store — substituting from a cache that serves a scratch store
    needs `?store=<dir>` in the substituter URL. Irrelevant in production
    (real runners use /nix/store) but required in hermetic tests.
    (b) `?trusted=true` is honored by the substitution goal (`nix-store
    --realise`, normal builds), but plain `nix copy --from` does not
    consult it; the equivalent there is `--no-check-sigs`. Both flows are
    covered by tests.
16. **Blocking subprocess calls deadlock single-threaded async tests**
    (Phase 4 test-infra finding). A `std::process::Command::output()`
    call inside a `#[tokio::test]` blocks the only runtime thread; if the
    subprocess (e.g. `nix copy`) talks to an axum server spawned on the
    same runtime, both wait on each other forever. Rule for all server
    tests: subprocesses go through `tokio::process`, and every test body
    is wrapped in `tokio::time::timeout` so a future regression fails
    instead of hanging the suite.
17. **GC integration tests fabricate store paths instead of using a Nix
    store** (Phase 5). GC operates purely on manifests and pack blobs, so
    `tests/support/sim.rs` drives the *real* chunker, pack uploads, and
    SaveMutable commits with synthetic path contents — no nix tooling
    needed. This keeps the 30-day simulation in the low seconds, makes the
    tests runnable inside the Nix build sandbox, and gives precise control
    over content sizes (liveness ratios) and timestamps. Readability
    assertions still go through the real substituter (narinfo + NAR hash
    check), so "path is alive" means "Nix could substitute it".

18. **Old manifest versions must be garbage-collected too** (Phase 5
    addition). Every drain and every GC commit creates a new `m#N` entry;
    the older versions are dead weight that would accumulate forever (one
    per CI run). `hestia gc` deletes all but the newest `m#` entry as its
    final execute step (entries must also be older than the safety age so
    in-flight readers keep working). The 30-day simulation asserts the
    manifest entry count stays bounded.

19. **Orphan deletion must be filtered against the committed manifest**
    (Phase 5 correctness finding). The orphan sweep ("in GitHub but in no
    manifest, older than 1h") can race with GC's own recovery: when a
    previous GC run crashed after uploading repack packs but before
    committing, the next run sees those packs as orphans *and* re-uses them
    as repack output (CAS: same content, same key). Deleting the planned
    orphans after the commit would then delete a pack the just-committed
    manifest references. Fix: orphan keys are re-filtered against the
    committed manifest inside the commit step. The crash-safety test (stop
    after every execute step, then recover) covers exactly this sequence.

20. **Eviction and orphan judgments need a minimum-age grace** (Phase 5).
    A pack missing from the REST listing is normally "evicted by GitHub",
    and a listed pack absent from the manifest is normally an orphan — but
    both can also be a concurrent push that has uploaded its pack and not
    yet committed its manifest (or committed it after GC listed the cache).
    Packs younger than `min_age` (1h) are therefore never judged evicted or
    orphaned. Timestamps come from the REST API's RFC 3339 strings; the
    parser/formatter is hand-rolled in `gha::rest` (no chrono dependency)
    and the fake backend emits the same format production does.

21. **A corrupt manifest degrades to an empty manifest, except in GC**
    (Phase 6). Manifest decode failures (truncated upload, garbage blob)
    previously failed every drain forever. Now `decode_manifest_or_empty`
    logs the error and returns an empty manifest: cached paths read as
    misses, Nix rebuilds them, and the next commit overwrites the corrupt
    version. A binary cache must never be the reason CI fails. GC is the
    one exception — it still fails loudly, because deleting packs based on
    unreadable state would turn a corruption into data loss.

22. **Token expiry gets a dedicated error** (Phase 6). Twirp HTTP 401 maps
    to `Error::TokenExpired` with an actionable message (the runtime JWT
    lives ~6h; re-running the job is the only fix) instead of a generic
    "unexpected HTTP status 401". No partial manifest is ever committed on
    the way down: SaveMutable only finalizes complete uploads.

23. **The substituter retries transient Azure failures** (Phase 6). Pack
    Range reads retry up to twice on network errors and 5xx responses
    before the NAR request turns into a 404. Auth failures (401/403) still
    take the URL-refresh path; missing blobs (eviction) still fail
    immediately. The "never partial or corrupt data" rule is unchanged.

24. **GetCacheEntryDownloadURL only matches restore keys** (Phase 6,
    real-API finding). The production service ignores the `key` field for
    matching; an exact-key request with empty `restore_keys` misses even
    when the entry exists. The client now always sends the key as the
    first restore key (go-actions-cache does the same), and the fake
    mimics the restore-keys-only matching so it cannot drift back. Caught
    by `tests/gha_real.rs` in the first CI run against the real service.

25. **The action is a single node24 JS action** (Phase 6, resolves
    Decision 5's caveat). Composite + nested-node-action workaround
    replaced by one JS action with a native `post:` hook, no
    `actions/github-script` dependency, and no npm dependencies at all
    (node builtins + workflow commands replace @actions/core). Install
    modes: sha256-verified release download, local binary path, or
    token-capture-only (used by hestia's own CI and real-API tests).

26. **Release binaries are static (musl)** (Phase 6). Nix-built dynamic
    binaries reference /nix/store library paths and cannot run on stock
    runners where the action downloads them. `packages.static` builds with
    pkgsStatic; rusqlite already bundles sqlite and reqwest uses rustls,
    so this needed no code changes. aarch64-linux builds natively on
    ubuntu-24.04-arm runners (no cross compilation).

27. **CI uses NixOS/nix-installer-action** (Phase 6). The Determinate
    Systems installer action increasingly diverges from upstream Nix (it
    installs Determinate Nix by default); the NixOS-org fork tracks
    upstream. It publishes no version tags yet, so it is pinned `@main`.

28. **The real cache service is eventually consistent for lookups**
    (Phase 6, real-API finding; surfaced as flaky token-probe runs).
    GetCacheEntryDownloadURL may not return a just-finalized entry, and a
    prefix lookup may return version N-1 shortly after N was committed.
    Reservations (CreateCacheEntry) are strongly consistent — conflicts are
    always detected. Consequences: (a) the real-API tests poll
    read-after-write assertions instead of asserting immediately; (b)
    SaveMutable's stale-skip window was widened to 60s (20 × 3s) so
    propagation lag is never misdiagnosed as a crashed writer; (c) the
    daemon practices **read-your-writes**: the pipeline publishes the
    manifest it committed directly into the substituter's `ManifestStore`
    (instead of re-loading it from the cache, which returned a stale
    version and made just-pushed paths 404 — caught by the action-test CI
    job), folds that manifest into every later merge base, and passes its
    version to `SaveMutable::save_with_floor` so a drain never fights its
    own previous commit in the conflict loop; (d) everything else
    tolerates lag by design — a stale read is a cache miss (rebuild),
    never corruption, and packs referenced by a manifest are always
    uploaded *before* the manifest commit. The fake backend simulates lag
    only behind the `stale-lookups` injection endpoint; everything else
    stays strongly consistent.

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
