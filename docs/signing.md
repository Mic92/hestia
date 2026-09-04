# Signing heads

A head is the only mutable pointer in a store, and everything it names is
content addressed and hash checked on read, so signing heads pins the whole
graph. It is the only signature hestia makes: NAR responses are served
unsigned by a substituter Nix has been told to trust.

Buckets and registries have no per-branch scopes, so anyone who can write can
publish a head into any root, `main-*` included. The trust policy is what
makes readers ignore those.

| head | signed payload |
|---|---|
| drain `h-*` | its own name (base epoch, root id, time, segment digest) |
| compaction `c-*` | the record (root, added and replaced segments, time) |
| GC `g-*` | the GC record |

The proof is a sigstore bundle from `cosign attest-blob --type
https://github.com/Mic92/hestia/head/v1`, stored as the head object's body.

## On GitHub Actions

`trust` signs this job's heads keyless with the workflow's OIDC identity and
writes the matching policy. The job needs `permissions: id-token: write`.

| `trust` | accepts |
|---|---|
| `open` (default) | everything, nothing is signed |
| `same-repo` | heads attested by any workflow of this repository |
| `strict` | `<default branch>-*` roots and GC records only from default branch runs, other roots from any ref of this repository |

Verification uses the runner's `gh attestation verify`, signing jobs download
cosign. Signers the preset does not cover go into `trust-rows`, which is
consulted first:

```yaml
          trust-rows: |
            main-* cosign --key hydra.pub --insecure-ignore-tlog=true
```

## Elsewhere

`hestia serve` and `hestia gc` read two variables:

* `HESTIA_SIGN`: `cosign attest-blob` arguments. Empty signs keyless and
  needs an OIDC identity, unset publishes unsigned heads.
* `HESTIA_TRUST`: one row per line, first matching glob wins.

  ```
  <root glob | @gc>  <cosign | gh>  <verify args…>
  ```

  `@gc` matches GC records. Rows run `cosign verify-blob-attestation` or
  `gh attestation verify` with those arguments. Unset accepts every head.

For a builder without an OIDC identity, sign with a key pair:

```console
$ COSIGN_PASSWORD= cosign generate-key-pair --output-key-prefix builder
$ cosign signing-config create --out signing.json
$ export COSIGN_PASSWORD=
$ export HESTIA_SIGN="--key builder.key --signing-config signing.json"
$ hestia serve --branch main
```

Readers and GC runs carry the public half:

```console
$ export HESTIA_TRUST="main-* cosign --key builder.pub --insecure-ignore-tlog=true
@gc cosign --key builder.pub --insecure-ignore-tlog=true"
$ hestia serve --branch main
```

`--insecure-ignore-tlog=true` keeps keyed signing offline, without it cosign
expects a transparency log entry. KMS keys work the same way,
`--key awskms:///arn:aws:kms:...` to sign, the public key to verify.

## Rejected heads

A head no row accepts counts as not listed. Nothing fails, the data behind it
is simply not served:

```
hestia: main-x86_64-linux: proof rejected by Cosign … accepted signatures do
not match threshold, Found: 0, Expected 1
```

If it was the only head naming a root, that root looks empty. Unsigned heads
in a root whose row demands a signature disappear the same way, while
correctly signed ones keep serving.

Rotating a key therefore hides everything the old one published. Keep both
rows until a run has republished under the new key:

```
main-* cosign --key new.pub --insecure-ignore-tlog=true
main-* cosign --key old.pub --insecure-ignore-tlog=true
```

## Verifying by hand

A drain head's body is the bundle and its payload is the head name.
Compaction and GC heads wrap record and bundle together, so pick an `h-*`
head; a root's first push publishes only a `c-*`.

```console
$ head=$(curl -s https://pub-<id>.r2.dev/index | grep ^h- | head -1)
$ curl -s "https://pub-<id>.r2.dev/heads/$head" -o head.bundle
$ printf %s "$head" > head.payload
$ cosign verify-blob-attestation \
    --type https://github.com/Mic92/hestia/head/v1 \
    --bundle head.bundle --key builder.pub \
    --insecure-ignore-tlog=true head.payload
Verified OK
```

Readers need `cosign` or `gh` on `PATH` and spawn one process per pending
head at load time.
