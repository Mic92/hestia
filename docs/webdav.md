# A WebDAV share as the store

For when the storage you already have is a Hetzner Storage Box, a NAS, or
an nginx/Apache directory, and standing up S3 for a CI cache is not worth
it. CI pushes over WebDAV with a password; readers use the same
URL, or plain HTTPS if the directory is also served that way.

The tree on the share is ordinary files (`pack/<xx>/`, `seg/<xx>/`, `heads/`,
`index`), the same a bucket would hold, so `rclone` can mirror it and
`HESTIA_S3=https://…` can read it.

## 1. A directory and a credential

**Hetzner Storage Box**: enable WebDAV in the Robot panel, create a
sub-account restricted to a directory. URL
`https://<user>.your-storagebox.de/hestia`.

**nginx**: needs `dav_ext` for `PROPFIND` (Debian: `libnginx-mod-http-dav-ext`,
NixOS: `services.nginx.additionalModules = [ pkgs.nginxModules.dav ]`):

```nginx
location /hestia/ {
    root /srv;
    auth_basic hestia; auth_basic_user_file /etc/nginx/hestia.htpasswd;
    dav_methods PUT DELETE MKCOL; dav_ext_methods PROPFIND OPTIONS;
    create_full_put_path off;
    client_max_body_size 128m;
}
```

**Apache**: `DavLockDB`, then `<Directory /srv/hestia> Dav On; AuthType Basic; … Require valid-user </Directory>`.

The directory the URL names must exist; hestia creates everything below
it. Store user and password as repository secrets.

## 2. Push from CI

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write # for trust: same-repo or strict
    steps:
      - uses: actions/checkout@v6
      - uses: NixOS/nix-installer-action@main
      - uses: Mic92/hestia@v3
        with:
          dav: https://dav.example.org/hestia
          trust: same-repo
        env:
          HESTIA_DAV_USER: ci
          HESTIA_DAV_PASSWORD: ${{ secrets.HESTIA_DAV_PASSWORD }}
      - run: nix build .#
```

A share has no per-branch scopes, so anyone holding the password can
publish into any root: `trust` decides whose heads readers believe, see
[signing](signing.md). Repositories share a directory by sub-path,
`…/hestia/<repo>` (create it first).

Fork pull requests get no secrets. Give them a read-only URL instead.

## 3. Read without the password

Either a second, read-only DAV account (`HESTIA_DAV_USER`/`_PASSWORD` of
that account, same `dav:` URL), or plain HTTPS if the directory is served
without auth for `GET`:

```console
$ HESTIA_S3=https://static.example.org/hestia hestia serve --branch main
```

or as an action input, `s3: https://static.example.org/hestia`. Heads
then come from the `index` file the writers keep, no `PROPFIND` needed, so
any static file server or CDN in front works; with nginx that is the same
`location` minus `auth_basic` for `GET` (`limit_except GET { auth_basic …; }`).

## 4. Collect garbage

GC needs the writing credential and lists `pack/` and `seg/` shard by
shard, 500-odd small `PROPFIND`s:

```yaml
on:
  schedule:
    - cron: "17 4 * * *"
jobs:
  gc:
    runs-on: ubuntu-latest
    env:
      HESTIA_DAV: https://dav.example.org/hestia
      HESTIA_DAV_USER: ci
      HESTIA_DAV_PASSWORD: ${{ secrets.HESTIA_DAV_PASSWORD }}
    steps:
      - uses: Mic92/hestia@v3
        with:
          read-only: true
      - run: $HESTIA_BIN gc
```

## Server notes

Tested against nginx dav_ext, Apache mod_dav, lighttpd mod_webdav, rclone
`serve webdav`, dufs and WsgiDAV.


- Request bodies up to ~70 MiB (one pack): nginx `client_max_body_size`,
  Apache `LimitRequestBody`.
- nginx ignores `If-Match`/`If-None-Match`, so two jobs rewriting `index`
  at once can drop a head from it until the next push rewrites it. Only
  plain-HTTPS readers notice; DAV readers list `heads/` directly.
- `Depth: infinity` is never used; servers may keep it disabled.

## Inspecting a store

```console
$ curl -su ci:$PW https://dav.example.org/hestia/index
$ curl -su ci:$PW -X PROPFIND -H 'Depth: 1' https://dav.example.org/hestia/heads/ | grep -o '<D:href>[^<]*'
$ rclone lsf dav:hestia/pack/ | head     # shard directories
```

A reader that reports a missing index is pointed at the wrong directory.
