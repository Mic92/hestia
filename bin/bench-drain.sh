#!/usr/bin/env bash
# Benchmark a real hestia drain against the local mock cache.
#
# Starts mock-cache, points a hestia daemon at it, registers the given store
# paths (and their closures) via the post-build hook, then times the drain.
#
# Usage:
#   bin/bench-drain.sh /nix/store/xxx-foo [/nix/store/yyy-bar ...]
#
# Binaries are taken from $HESTIA_BIN / $MOCK_BIN if set, else from
# ./target/release (build them first: cargo build --release).
set -euo pipefail

if [ "$#" -lt 1 ]; then
	echo "usage: $0 <store-path> [store-path ...]" >&2
	exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
hestia="${HESTIA_BIN:-$root/target/release/hestia}"
mock="${MOCK_BIN:-$root/target/release/mock-cache}"

for bin in "$hestia" "$mock"; do
	if [ ! -x "$bin" ]; then
		echo "missing $bin (run: cargo build --release)" >&2
		exit 1
	fi
done

work="$(mktemp -d)"
socket="$work/hestia.sock"
addr="127.0.0.1:8099"
pids=()

cleanup() {
	for pid in "${pids[@]:-}"; do
		[ -n "$pid" ] && kill "$pid" 2>/dev/null || true
	done
	rm -rf "$work"
}
trap cleanup EXIT

# 1. Mock cache backend.
"$mock" --addr "$addr" --data-dir "$work/blobs" >"$work/mock.log" 2>&1 &
pids+=("$!")

# 2. Point hestia at it.
eval "$("$mock" --print-env --addr "$addr")"
export ACTIONS_RESULTS_URL ACTIONS_RUNTIME_TOKEN GITHUB_API_URL GITHUB_TOKEN GITHUB_REPOSITORY

# 3. Daemon.
"$hestia" serve --socket "$socket" --listen "127.0.0.1:8100" >"$work/serve.log" 2>&1 &
pids+=("$!")

for _ in $(seq 1 50); do
	[ -S "$socket" ] && break
	sleep 0.1
done
if [ ! -S "$socket" ]; then
	echo "daemon socket did not appear; serve.log:" >&2
	cat "$work/serve.log" >&2
	exit 1
fi

# 4. Register paths and time the drain.
"$hestia" hook --socket "$socket" "$@"

echo "draining $# path(s)..." >&2
start=$(date +%s.%N)
"$hestia" drain --socket "$socket"
end=$(date +%s.%N)

blob_bytes=$(du -sb "$work/blobs" | cut -f1)
printf 'drain wall time: %.2fs, uploaded %s bytes\n' \
	"$(echo "$end - $start" | bc)" "$blob_bytes"
