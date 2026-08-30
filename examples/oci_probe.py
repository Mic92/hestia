#!/usr/bin/env python3
"""Probe an OCI registry for what the hestia OCI backend relies on.

Checks: token auth via WWW-Authenticate challenge, monolithic + chunked
blob upload of non-image bytes, OCI manifest with custom artifactType,
large annotations (drain proof) and config blobs (head records), tag
listing lag / pagination / order, blob GET redirect + Range, tag
overwrite semantics, If-Match, manifest by digest after retag,
referrers, DELETE, latency of each hop.

usage: oci_probe.py <registry>/<repo>   e.g. ghcr.io/owner/repo, docker.io/user/repo, localhost:5000/test
creds: OCI_USER/OCI_PASSWORD, else ~/.docker/config.json, else GH_TOKEN for ghcr.io
"""

import base64
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

MT_MANIFEST = "application/vnd.oci.image.manifest.v1+json"


def sha(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):  # type: ignore[override]
        return None


opener_noredir = urllib.request.build_opener(NoRedirect)

Resp = tuple[int, dict[str, str], bytes, float]


def req(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    follow: bool = True,
    ok: tuple[int, ...] | None = (200, 201, 202, 204, 206, 307),
) -> Resp:
    r = urllib.request.Request(url, method=method, data=data, headers=headers or {})
    t0 = time.monotonic()
    try:
        resp = (urllib.request.urlopen if follow else opener_noredir.open)(
            r, timeout=60
        )
        status, hdrs, body = resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        status, hdrs, body = e.code, dict(e.headers), e.read()
    dt = time.monotonic() - t0
    if ok is not None and status not in ok:
        raise SystemExit(f"{method} {url} -> {status}\n{body[:400]!r}")
    return status, {k.lower(): v for k, v in hdrs.items()}, body, dt


def credentials(host: str) -> tuple[str, str] | None:
    if "OCI_USER" in os.environ:
        return os.environ["OCI_USER"], os.environ["OCI_PASSWORD"]
    cfg = Path(os.environ.get("DOCKER_CONFIG", Path.home() / ".docker")) / "config.json"
    if cfg.exists():
        auths = json.loads(cfg.read_text()).get("auths", {})
        for k in (
            host,
            f"https://{host}",
            "https://index.docker.io/v1/" if host == "registry-1.docker.io" else "",
        ):
            a = auths.get(k, {}).get("auth")
            if a:
                u, p = base64.b64decode(a).decode().split(":", 1)
                return u, p
    if host == "ghcr.io" and "GH_TOKEN" in os.environ:
        return "token", os.environ["GH_TOKEN"]
    return None


class Registry:
    def __init__(self, ref: str) -> None:
        host, _, repo = ref.partition("/")
        if host == "docker.io":
            host = "registry-1.docker.io"
            if "/" not in repo:
                repo = "library/" + repo
        self.host, self.repo = host, repo.lower()
        scheme = "http" if host.split(":")[0] in ("localhost", "127.0.0.1") else "https"
        self.base = f"{scheme}://{host}"
        self.v2 = f"{self.base}/v2/{self.repo}"
        self.auth: dict[str, str] = {}
        self.creds = credentials(host)

    def login(self) -> None:
        st, h, _, dt = req("GET", f"{self.base}/v2/", ok=None)
        ch = h.get("www-authenticate", "")
        if st == 200:
            print(f"auth: none required ({dt * 1e3:.0f} ms)")
            return
        if not ch.lower().startswith("bearer"):
            if self.creds:
                b = base64.b64encode(":".join(self.creds).encode()).decode()
                self.auth = {"Authorization": f"Basic {b}"}
            print(f"auth: basic ({ch!r})")
            return
        kv = {
            k.strip(): v.strip('"')
            for k, v in (p.split("=", 1) for p in ch[len("bearer ") :].split(","))
        }
        q = {"scope": f"repository:{self.repo}:pull,push"}
        if "service" in kv:
            q["service"] = kv["service"]
        hdr = {}
        if self.creds:
            b = base64.b64encode(":".join(self.creds).encode()).decode()
            hdr = {"Authorization": f"Basic {b}"}
        _, _, body, dt = req(
            "GET", f"{kv['realm']}?{urllib.parse.urlencode(q)}", headers=hdr
        )
        j = json.loads(body)
        self.auth = {"Authorization": f"Bearer {j.get('token') or j['access_token']}"}
        print(
            f"auth: bearer via {urllib.parse.urlparse(kv['realm']).netloc} ({dt * 1e3:.0f} ms)"
        )

    def loc(self, h: dict[str, str]) -> str:
        return urllib.parse.urljoin(self.base, h["location"])

    def put_blob(self, blob: bytes) -> tuple[int, float]:
        _, h, _, dt = req(
            "POST", f"{self.v2}/blobs/uploads/", headers=self.auth, data=b""
        )
        loc = self.loc(h)
        sep = "&" if "?" in loc else "?"
        st, _, _, dt2 = req(
            "PUT",
            f"{loc}{sep}digest={sha(blob)}",
            headers={
                **self.auth,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(blob)),
            },
            data=blob,
        )
        return st, dt + dt2

    def put_manifest(self, ref: str, m: bytes, ok: tuple[int, ...] = (201,)) -> Resp:
        return req(
            "PUT",
            f"{self.v2}/manifests/{ref}",
            headers={**self.auth, "Content-Type": MT_MANIFEST},
            data=m,
            ok=ok,
        )

    def get_manifest(self, ref: str, ok: tuple[int, ...] = (200,)) -> Resp:
        return req(
            "GET",
            f"{self.v2}/manifests/{ref}",
            headers={**self.auth, "Accept": MT_MANIFEST},
            ok=ok,
        )

    def tags(
        self, n: int | None = None, last: str | None = None
    ) -> tuple[list[str], dict[str, str], float]:
        q = {k: str(v) for k, v in (("n", n), ("last", last)) if v is not None}
        url = f"{self.v2}/tags/list" + (f"?{urllib.parse.urlencode(q)}" if q else "")
        _, h, body, dt = req("GET", url, headers=self.auth, ok=(200, 404))
        try:
            t = json.loads(body).get("tags") or []
        except json.JSONDecodeError:
            t = []
        return t, h, dt


def manifest(
    cfg: tuple[str, int],
    layers: list[tuple[str, int, str]],
    annotations: dict[str, str],
) -> bytes:
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": MT_MANIFEST,
            "artifactType": "application/vnd.hestia.segment.v1",
            "config": {
                "mediaType": "application/vnd.hestia.config.v1+cbor",
                "digest": cfg[0],
                "size": cfg[1],
            },
            "layers": [
                {"mediaType": mt, "digest": d, "size": n} for d, n, mt in layers
            ],
            "annotations": annotations,
        },
        separators=(",", ":"),
    ).encode()


def main() -> None:
    r = Registry(sys.argv[1])
    print(f"registry {r.host} repo {r.repo}")
    r.login()
    run = hashlib.sha256(os.urandom(8)).hexdigest()[:8]

    # blobs
    seg = os.urandom(300_000) + b"hestia-probe-segment"
    st, dt = r.put_blob(seg)
    print(f"blob POST+PUT {len(seg)} B -> {st} ({dt * 1e3:.0f} ms)")

    cfg_small = b'{"hestia":"probe"}'
    r.put_blob(cfg_small)
    cfg_big = json.dumps(
        {"roots": {f"root-{i}": [sha(bytes([i % 256]))] for i in range(20_000)}}
    ).encode()
    st, dt = r.put_blob(cfg_big)
    print(
        f"config blob {len(cfg_big) >> 10} KiB (g-* with 20k roots) -> {st} ({dt * 1e3:.0f} ms)"
    )

    pack = os.urandom(2 * 1024 * 1024 + 123)
    _, h, _, _ = req("POST", f"{r.v2}/blobs/uploads/", headers=r.auth, data=b"")
    loc, off, t0, chunked = r.loc(h), 0, time.monotonic(), True
    for part in (pack[: 1 << 20], pack[1 << 20 :]):
        st, h, body, _ = req(
            "PATCH",
            loc,
            headers={
                **r.auth,
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(part)),
                "Content-Range": f"{off}-{off + len(part) - 1}",
            },
            data=part,
            ok=(202, 400, 404, 405, 416),
        )
        if st != 202:
            chunked = False
            print(f"chunked PATCH -> {st} {body[:120]!r}")
            break
        loc, off = r.loc(h), off + len(part)
    if chunked:
        sep = "&" if "?" in loc else "?"
        st, _, _, _ = req(
            "PUT",
            f"{loc}{sep}digest={sha(pack)}",
            headers={**r.auth, "Content-Length": "0"},
            data=b"",
        )
        print(
            f"chunked PATCH×2+PUT {len(pack)} B -> {st} ({(time.monotonic() - t0) * 1e3:.0f} ms)"
        )
    else:
        r.put_blob(pack)

    st, h, _, dt = req(
        "HEAD",
        f"{r.v2}/blobs/{sha(seg)}",
        headers=r.auth,
        follow=False,
        ok=(200, 307, 404),
    )
    print(f"HEAD blob -> {st} len={h.get('content-length')} ({dt * 1e3:.0f} ms)")

# manifests: annotation sizes (drain proof is a ~1.5 KB JWT), find the cap
    tag = f"h-1-0-{run}"
    md = {}
    for label, size in (
        ("1.5K", 1500),
        ("16K", 16_000),
        ("256K", 256_000),
        ("3.9M", 3_900_000),
    ):
        m = manifest(
            (sha(cfg_small), len(cfg_small)),
            [(sha(seg), len(seg), "application/vnd.hestia.tree.v1")],
            {
                "io.hestia.root": "main-x86_64-linux",
                "io.hestia.base_gen": "1",
                "io.hestia.proof": "A" * size,
            },
        )
        st, h, body, dt = r.put_manifest(
            tag if size == 1500 else f"probe-ann-{run}", m, ok=(201, 400, 413, 404)
        )
        print(
            f"PUT manifest annotation {label} (total {len(m) >> 10} KiB) -> {st} ({dt * 1e3:.0f} ms) {body[:80].decode(errors='replace') if st != 201 else ''}"
        )
        if size == 1500:
            md["h"] = h.get("docker-content-digest") or sha(m)
            t_pushed = time.monotonic()
        if st != 201:
            break

    # listing lag: how long until the new tag is in tags/list
    lag = None
    for i in range(60):
        t, h, dt = r.tags()
        if tag in t:
            lag = time.monotonic() - t_pushed
            break
        time.sleep(1)
    print(
        f"tags/list lag: {f'{lag:.1f} s' if lag is not None else '>60 s'} ({len(t)} tags, {dt * 1e3:.0f} ms/call)"
    )

    # big config blob manifest (g-*)
    mg = manifest((sha(cfg_big), len(cfg_big)), [], {"io.hestia.proof": "A" * 1500})
    st, h, body, dt = r.put_manifest(f"g-1-{run}", mg, ok=(201, 400, 413))
    md["g"] = h.get("docker-content-digest") or sha(mg)
    print(
        f"PUT manifest with {len(cfg_big) >> 10} KiB config blob, no layers -> {st} ({dt * 1e3:.0f} ms) {body[:80].decode(errors='replace') if st != 201 else ''}"
    )

    # untagged manifest by digest
    mp = manifest(
        (sha(cfg_small), len(cfg_small)),
        [(sha(pack), len(pack), "application/vnd.hestia.pack.v1")],
        {},
    )
    st, _, body, dt = r.put_manifest(sha(mp), mp, ok=(201, 400, 404, 405))
    print(f"PUT manifest by digest (untagged) -> {st} ({dt * 1e3:.0f} ms)")

    # pagination + order
    for i in range(5):
        r.put_manifest(f"c-1-0-{run}-{4 - i}", mp)
    full, h, dt = r.tags()
    page, hp, _ = r.tags(n=3)
    ours = [t for t in full if t.endswith(tuple(f"{run}-{i}" for i in range(5)))]
    order = (
        "lexical"
        if ours == sorted(ours)
        else (
            "insertion"
            if ours == [f"c-1-0-{run}-{4 - i}" for i in range(5)]
            else "other"
        )
    )
    print(
        f"tags/list: {len(full)} tags ({dt * 1e3:.0f} ms), n=3 honoured={len(page) == 3}, Link header={'link' in hp}, order={order}"
    )
    if len(page) == 3:
        page2, _, _ = r.tags(n=3, last=page[-1])
        print(f"  page 2 via last= -> {page2[:3]}")

    # blob GET: redirect? range?
    st, h, body, dt = req(
        "GET",
        f"{r.v2}/blobs/{sha(seg)}",
        headers=r.auth,
        follow=False,
        ok=(200, 307, 302),
    )
    cdn = h.get("location", "")
    print(
        f"GET blob -> {st} {'redirect to ' + urllib.parse.urlparse(cdn).netloc if cdn else 'direct ' + str(len(body)) + ' B'} ({dt * 1e3:.0f} ms)"
    )
    if cdn:
        st, h, body, dt = req("GET", cdn, headers={"Range": "bytes=100-4195"})
        print(
            f"Range on CDN -> {st} got={len(body)} B match={body == seg[100:4196]} ({dt * 1e3:.0f} ms)"
        )
        exp = urllib.parse.parse_qs(urllib.parse.urlparse(cdn).query)
        for k in ("se", "X-Amz-Expires", "Expires", "exp"):
            if k in exp:
                print(f"  signed URL expiry {k}={exp[k][0]}")
    st, h, body, dt = req(
        "GET", f"{r.v2}/blobs/{sha(seg)}", headers={**r.auth, "Range": "bytes=100-4195"}
    )
    print(
        f"Range via registry (follow) -> {st} got={len(body)} B match={body == seg[100:4196]} ({dt * 1e3:.0f} ms)"
    )

    # manifest GET latency
    st, _, body, dt = r.get_manifest(tag)
    print(f"GET manifest by tag -> {st} {len(body)} B ({dt * 1e3:.0f} ms)")
    st, _, body, dt = r.get_manifest(md["g"])
    _, _, _, dt2 = req("GET", f"{r.v2}/blobs/{sha(cfg_big)}", headers=r.auth)
    print(f"GET g-* manifest + config blob -> {dt * 1e3:.0f}+{dt2 * 1e3:.0f} ms")

    # tag overwrite
    m2 = manifest((sha(cfg_small), len(cfg_small)), [], {"note": "overwrite"})
    st, h, body, _ = r.put_manifest(tag, m2, ok=(201, 400, 409, 412))
    if st == 201:
        _, _, body, _ = r.get_manifest(tag)
        print(
            f"tag overwrite -> {st}, tag now {'new (LWW)' if sha(body) == sha(m2) else 'OLD (first wins?)'}",
            end="; ",
        )
        st, _, _, _ = r.get_manifest(md["h"], ok=(200, 404))
        print(f"old manifest by digest -> {st}")
    else:
        print(f"tag overwrite -> {st} (immutable tags) {body[:80]!r}")
    st, _, _, _ = req(
        "PUT",
        f"{r.v2}/manifests/{tag}",
        headers={**r.auth, "Content-Type": MT_MANIFEST, "If-Match": '"sha256:00"'},
        data=m2,
        ok=(201, 412, 400),
    )
    print(f"PUT with bogus If-Match -> {st} ({'CAS!' if st == 412 else 'ignored'})")

    st, _, _, _ = req("GET", f"{r.v2}/referrers/{md['h']}", headers=r.auth, ok=None)
    print(f"referrers API -> {st}")

    # delete
    st, _, body, _ = req(
        "DELETE", f"{r.v2}/manifests/{md['g']}", headers=r.auth, ok=None
    )
    print(
        f"DELETE manifest -> {st} {body[:100].decode(errors='replace') if st not in (202, 200) else ''}"
    )
    st, _, body, _ = req("DELETE", f"{r.v2}/blobs/{sha(pack)}", headers=r.auth, ok=None)
    print(f"DELETE blob -> {st}")
    if st in (200, 202):
        st, _, _, _ = req(
            "HEAD", f"{r.v2}/blobs/{sha(pack)}", headers=r.auth, follow=False, ok=None
        )
        print(f"  HEAD after delete -> {st}")

    print(f"\ndone. probe tags carry suffix {run}.")


if __name__ == "__main__":
    main()
