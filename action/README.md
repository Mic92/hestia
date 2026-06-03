# hestia-cache action

> ⚠️ **Alpha software**: APIs, cache format, and behavior may change without
> notice. Not yet recommended for production CI.

This action runs [hestia](https://github.com/Mic92/hestia) inside your job,
turning the GitHub Actions cache into a Nix binary cache.

When the job starts, the action:

1. Captures the Actions cache API tokens (`ACTIONS_RUNTIME_TOKEN`,
   `ACTIONS_RESULTS_URL`). They are only visible to JS actions, which is why
   hestia needs an action at all and cannot be set up from `run:` steps.
2. Installs the `hestia` binary, either from a GitHub release (verified
   against GitHub's build attestations) or from a path you built yourself.
3. Starts the hestia daemon: a post-build-hook listener plus a local
   substituter speaking the Nix binary cache protocol over HTTP.
4. Wires both into `nix.conf` (`extra-substituters` with `?trusted=true`,
   `post-build-hook`) and restarts the nix-daemon if there is one.

When the job ends, a post step drains the daemon: everything that was built
is chunked, packed, and uploaded, and the manifest is committed to the GHA
cache.

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
          version: v0.1.0-alpha.7
      - run: nix build .#
```

Later runs substitute what earlier runs built instead of rebuilding it.

### Using a locally built binary

If you build hestia yourself, while hacking on it or because you do not want
to trust release binaries, pass a path instead:

```yaml
      - run: nix build github:Mic92/hestia -o hestia-bin
      - uses: Mic92/hestia/action@main
        with:
          binary: ./hestia-bin/bin/hestia
```

### Token capture only

With neither `version` nor `binary` set, the action only exports the cache
API tokens and starts nothing. This mode is for setups that run hestia
themselves; hestia's own integration tests use it.

## Inputs

| Input | Default | Description |
|---|---|---|
| `binary` | — | Path to a pre-built hestia binary. Takes precedence over `version`. |
| `version` | — | Release tag to download (e.g. `v0.1.0-alpha.4`). The download is verified against GitHub's build attestations. |
| `github-token` | `${{ github.token }}` | Token for the attestation API lookup. |
| `listen` | `127.0.0.1:37515` | Substituter listen address. |
| `socket` | `/tmp/hestia/hook.sock` | Post-build-hook unix socket path. |
| `drain-timeout` | `300` | Seconds the post-job step waits for the final upload. |
| `upstream-cache-filter` | `false` | Skip paths signed by an upstream cache instead of caching them (saves quota for big closures). |
| `upstream-cache-key-names` | `cache.nixos.org-1` | Space-separated key names treated as upstream caches by the filter. |
| `no-closure` | `false` | Cache built paths only, without their runtime closure. |

## Garbage collection

The cache needs a periodic GC run on the default branch: PR-scoped caches die
with their branch, but the default branch scope grows forever unless
something prunes it. Copy
[`.github/workflows/gc.yml`](../.github/workflows/gc.yml) from the hestia
repository as a starting point.

## Permissions

The job needs:

```yaml
permissions:
  actions: write    # GHA cache writes (uploads) and deletes (GC)
  contents: read
```
