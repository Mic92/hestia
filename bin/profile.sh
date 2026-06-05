#!/usr/bin/env bash
# Record the whole system with perf between `start` and `stop`, then render
# a flamegraph.
#
#   profile.sh start                 begin recording
#   profile.sh stop <output.svg>     end recording, render the flamegraph
#
# System-wide recording captures the hestia daemon, the command driving it,
# and kernel time; the flamegraph groups stacks by process. `stop` needs
# perf and the inferno tools on PATH.

set -euo pipefail

perf_data=/tmp/hestia-perf.data
perf_pid_file=/tmp/hestia-perf.pid

start() {
  if [[ -e $perf_pid_file ]]; then
    echo "recording already running (pid $(cat "$perf_pid_file"))" >&2
    exit 1
  fi
  sudo perf record -F 99 --call-graph dwarf -a -o "$perf_data" &
  local pid=$!
  # Let perf finish setting up before the caller starts the workload, and
  # catch immediate failures (bad options, missing permissions) here
  # instead of as a confusing kill error in stop.
  sleep 1
  if ! sudo kill -0 "$pid" 2>/dev/null; then
    wait "$pid" || true
    echo "perf record exited immediately; see its output above" >&2
    exit 1
  fi
  echo "$pid" >"$perf_pid_file"
  echo "recording started (pid $pid)" >&2
}

stop() {
  local output="${1:?usage: $0 stop <output.svg>}"
  if [[ ! -e $perf_pid_file ]]; then
    echo "no recording is running (start one with: $0 start)" >&2
    exit 1
  fi
  # Check the render tools before teardown: failing after the pid file is
  # removed would leave the captured data unrenderable through this script.
  local tool
  for tool in perf inferno-collapse-perf inferno-flamegraph; do
    if ! command -v "$tool" >/dev/null; then
      echo "$tool not found on PATH; install it before stopping" >&2
      exit 1
    fi
  done
  local pid
  pid=$(cat "$perf_pid_file")

  # SIGINT makes perf flush its buffers and exit cleanly.
  sudo kill -INT "$pid"
  tail --pid="$pid" -f /dev/null
  rm -f "$perf_pid_file"
  sudo chown "$(id -u)" "$perf_data"

  local title
  title=$(basename "$output" .svg)
  # Stream perf script into the collapser: its text output is many times
  # the size of perf.data and must not land on disk.
  perf script -i "$perf_data" |
    inferno-collapse-perf |
    inferno-flamegraph --title "$title" >"$output"
  rm -f "$perf_data"
  echo "flamegraph written to $output" >&2
}

case "${1:-}" in
  start) start ;;
  stop)
    shift
    stop "$@"
    ;;
  *)
    echo "usage: $0 start | stop <output.svg>" >&2
    exit 1
    ;;
esac
