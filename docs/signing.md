# Signing heads

A head is the only mutable pointer in a store: it names the segment a
writer published for a root, and everything below it is content addressed
and hash checked on read. Signing the head therefore pins the whole graph,
and it is the only signature hestia makes. NAR responses stay unsigned; the
substituter is registered as a trusted store, and integrity comes from the
NAR hash and the chunk hashes.

This matters wherever a store has no per-branch scopes. In an S3 bucket or
an OCI registry, anyone who can write can publish a head into any root,
including `main-*`. A trust policy is what makes readers ignore those.

| head | payload that is signed |
|---|---|
| drain `h-*` | its own name (base epoch, root id, time, segment digest) |
| compaction `c-*` | the record (root, added and replaced segments, time) |
| GC `g-*` | the GC record |

The proof is a sigstore bundle from `cosign attest-blob --type
https://github.com/Mic92/hestia/head/v1`, stored as the head object's body.

## On GitHub Actions

Set `trust`; the action signs this job's heads keyless with the workflow's
OIDC identity and writes the matching verification policy:

```yaml
      - uses: Mic92/hestia@v3
        with:
          s3: s3://hestia-cache
          s3-endpoint: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
          trust: strict
        env:
          AWS_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
          AWS_REGION: auto
```

The job needs `permissions: id-token: write`. Presets:

| `trust` | accepts |
|---|---|
| `open` (default) | everything, nothing is signed |
| `same-repo` | heads attested by any workflow of this repository |
| `strict` | `<default branch>-*` roots and GC records only from default branch runs, other roots from any ref of this repository |

Verification uses the runner's preinstalled `gh attestation verify`; signing
jobs download cosign. Add rows for signers the preset does not cover with
`trust-rows`, one per line, consulted first:

```yaml
          trust-rows: |
            main-* cosign --key hydra.pub --insecure-ignore-tlog=true
```

## Outside GitHub

Two environment variables, both read by `hestia serve` and `hestia gc`:

* `HESTIA_SIGN`: arguments for `cosign attest-blob`. Empty means keyless,
  which needs an OIDC identity. Unset publishes unsigned heads.
* `HESTIA_TRUST`: the policy, one row per line

  ```
  <root glob | @gc>  <cosign | gh>  <verify args…>
  ```

  First matching glob wins, `@gc` matches GC records. `cosign` rows run
  `cosign verify-blob-attestation`, `gh` rows run `gh attestation verify`.
  Unset accepts every head.

With a key pair, for a builder that has no OIDC identity:

```console
$ COSIGN_PASSWORD= cosign generate-key-pair --output-key-prefix builder
$ cosign signing-config create --out signing.json
```

The writer signs:

```console
$ export COSIGN_PASSWORD=
$ export HESTIA_SIGN="--key builder.key --signing-config signing.json"
$ hestia serve --branch main
```

and every reader, plus every GC run, carries the public half:

```console
$ export HESTIA_TRUST="main-* cosign --key builder.pub --insecure-ignore-tlog=true
@gc cosign --key builder.pub --insecure-ignore-tlog=true"
$ hestia serve --branch main
```

`--insecure-ignore-tlog=true` is what keeps keyed signing offline; without
it cosign expects a transparency log entry. A KMS key works the same way:
`--key awskms:///arn:aws:kms:...` for signing, the public key for verifying.

## What a rejected head looks like

A head whose proof no row accepts counts as not listed. Nothing errors, the
data behind it simply is not served:

```
hestia: main-x86_64-linux: proof rejected by Cosign … accepted signatures do
not match threshold, Found: 0, Expected 1
```

If that head was the only one naming a root, the store looks empty for that
root. Unsigned heads pushed into a root whose row demands a signature are
ignored the same way, while paths from properly signed heads keep serving.

**Rotating a key therefore hides everything the old key published.** List
both public keys while the rotation is in flight:

```
main-* cosign --key new.pub --insecure-ignore-tlog=true
main-* cosign --key old.pub --insecure-ignore-tlog=true
```

and drop the old row once a run has republished under the new key.

## Checking a policy by hand

The body of a drain head is the bundle itself, and its payload is the head
name. Compaction (`c-*`) and GC (`g-*`) heads wrap record and bundle
together, so pick an `h-*` head; a root's very first push publishes only a
`c-*`, later drains add `h-*`:

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

Readers need `cosign` or `gh` on `PATH`, one process per pending head at
load time.
