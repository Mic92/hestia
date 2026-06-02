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
  sudo perf record -F 199 --call-graph dwarf,16384 -a -o "$perf_data" &
  echo $! >"$perf_pid_file"
  # Let perf finish setting up before the caller starts the workload.
  sleep 1
  echo "recording started (pid $(cat "$perf_pid_file"))" >&2
}

stop() {
  local output="${1:?usage: $0 stop <output.svg>}"
  local pid
  pid=$(cat "$perf_pid_file")

  # SIGINT makes perf flush its buffers and exit cleanly.
  sudo kill -INT "$pid"
  tail --pid="$pid" -f /dev/null
  rm -f "$perf_pid_file"
  sudo chown "$(id -u)" "$perf_data"

  local title
  title=$(basename "$output" .svg)
  perf script -i "$perf_data" >"$perf_data.txt"
  inferno-collapse-perf <"$perf_data.txt" |
    inferno-flamegraph --title "$title" >"$output"
  rm -f "$perf_data" "$perf_data.txt"
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
