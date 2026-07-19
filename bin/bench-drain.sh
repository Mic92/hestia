#!/usr/bin/env bash
# Benchmark a real hestia drain and closure export against the local mock
# cache.
#
# Starts mock-cache, points a hestia daemon at it, registers the given store
# paths (and their closures) via the post-build hook, times the drain, then
# times downloading the same closure back out through GET /closure/<hash>
# (the prefetch endpoint build jobs use).
#
# Usage:
#   bin/bench-drain.sh [--perf] <store-path> [store-path ...]
#
# With --perf the daemon runs under `perf record` (call-graph dwarf) and
# writes perf.data in the repo root for `perf report`.
#
# Binaries are taken from $HESTIA_BIN / $MOCK_BIN if set, else from
# ./target/release (build them first; --perf wants debuginfo:
# CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release).
set -euo pipefail

perf=0
if [ "${1:-}" = "--perf" ]; then
	perf=1
	shift
fi

if [ "$#" -lt 1 ]; then
	echo "usage: $0 [--perf] <store-path> [store-path ...]" >&2
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

# 3. Daemon, optionally under perf.
serve=("$hestia" serve --socket "$socket" --listen "127.0.0.1:8100")
if [ "$perf" -eq 1 ]; then
	perf record -g --call-graph dwarf -o "$root/perf.data" -- \
		"${serve[@]}" >"$work/serve.log" 2>&1 &
else
	"${serve[@]}" >"$work/serve.log" 2>&1 &
fi
serve_launcher=$!
pids+=("$serve_launcher")

for _ in $(seq 1 100); do
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

# 5. Closure export (prefetch) of the same roots, one request.
hashes=$(for path in "$@"; do basename "$path" | cut -c1-32; done | paste -sd,)
start=$(date +%s.%N)
export_bytes=$(curl -fsS "http://127.0.0.1:8100/closure/$hashes" | wc -c)
end=$(date +%s.%N)
printf 'closure export wall time: %.2fs, %s bytes\n' \
	"$(echo "$end - $start" | bc)" "$export_bytes"

if [ "$perf" -eq 1 ]; then
	# Graceful shutdown lets perf finalize perf.data.
	serve_pid=$(pgrep -P "$serve_launcher" -f 'hestia serve' || true)
	[ -n "$serve_pid" ] && kill -TERM "$serve_pid" 2>/dev/null || true
	wait "$serve_launcher" 2>/dev/null || true
	echo "wrote $root/perf.data" >&2
fi
