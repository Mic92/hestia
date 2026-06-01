# hestia-cache action

> ⚠️ **Alpha software**: APIs, cache format, and behavior may change without
> notice. Not yet recommended for production CI.

GitHub Action that turns the GitHub Actions cache into a Nix binary cache,
powered by [hestia](https://github.com/Mic92/hestia).

What it does, in order:

1. Captures the Actions cache API tokens (`ACTIONS_RUNTIME_TOKEN`,
   `ACTIONS_RESULTS_URL`). These are only visible to JS actions — this is why
   hestia needs an action and cannot be set up from `run:` steps alone.
2. Installs the `hestia` binary (from a GitHub release, sha256-verified, or
   from a path you built yourself).
3. Starts the hestia daemon: a post-build-hook listener plus a local
   substituter (Nix binary cache protocol over HTTP).
4. Wires both into `nix.conf` (`extra-substituters` with `?trusted=true`,
   `post-build-hook`) and restarts the nix-daemon if there is one.
5. **Post-job**: drains the daemon — chunks, packs, and uploads everything
   that was built, then commits the manifest to the GHA cache.

## Usage

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      actions: write        # needed to write to the GHA cache
    steps:
      - uses: actions/checkout@v6
      - uses: NixOS/nix-installer-action@main
      - uses: Mic92/hestia/action@main
        with:
          version: v0.1.0-alpha.1
          sha256: <sha256 of the release binary>
      - run: nix build .#
```

Subsequent runs substitute everything the first run built from the GHA cache
instead of rebuilding it.

### Using a locally built binary

If you build hestia yourself (e.g. while hacking on it, or to avoid trusting
release binaries), pass a path instead:

```yaml
      - run: nix build github:Mic92/hestia -o hestia-bin
      - uses: Mic92/hestia/action@main
        with:
          binary: ./hestia-bin/bin/hestia
```

### Token capture only

With neither `version` nor `binary` set, the action only exports the cache
API tokens and starts nothing. This exists for setups that run hestia
themselves (hestia's own integration tests use it).

## Inputs

| Input | Default | Description |
|---|---|---|
| `binary` | — | Path to a pre-built hestia binary. Takes precedence over `version`. |
| `version` | — | Release tag to download (e.g. `v0.1.0-alpha.1`). Requires `sha256`. |
| `sha256` | — | Expected SHA-256 of the downloaded binary. Downloads are refused without it. |
| `listen` | `127.0.0.1:37515` | Substituter listen address. |
| `socket` | `/tmp/hestia/hook.sock` | Post-build-hook unix socket path. |
| `drain-timeout` | `300` | Seconds the post-job step waits for the final upload. |
| `upstream-cache-filter` | `false` | Skip paths signed by an upstream cache instead of caching them (saves quota for big closures). |
| `upstream-cache-key-names` | `cache.nixos.org-1` | Space-separated key names treated as upstream caches by the filter. |
| `no-closure` | `false` | Cache built paths only, without their runtime closure. |

## Garbage collection

The cache needs a periodic GC run on the default branch (PR-scoped caches die
with their branch, but the default branch scope grows forever otherwise). See
[`.github/workflows/gc.yml`](../.github/workflows/gc.yml) in the hestia
repository for a ready-to-copy workflow.

## Permissions

The job needs:

```yaml
permissions:
  actions: write    # GHA cache writes (uploads) and deletes (GC)
  contents: read
```
