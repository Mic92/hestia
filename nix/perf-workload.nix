# Synthetic workload for the Perf workflow (perf.yml): what the profiled
# drain chunks, packs, and uploads.
#
# 64 x 4 MiB random files (incompressible) plus 1000 small text files,
# ~256 MiB. PERF_WORKLOAD_ID keeps the derivation unique per run, so its
# chunks never deduplicate against earlier runs.
#
# Impure: reads PERF_WORKLOAD_ID and builtins.currentSystem, and fetches
# the nixpkgs revision pinned in flake.lock.
let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);
  node = lock.nodes.nixpkgs.locked;
  nixpkgs = fetchTarball {
    url = "${node.url}/archive/${node.rev}.tar.gz";
    sha256 = node.narHash;
  };
  pkgs = import nixpkgs { system = builtins.currentSystem; };
  workloadId = builtins.getEnv "PERF_WORKLOAD_ID";
in
assert workloadId != "";
pkgs.runCommand "perf-workload-${workloadId}" { } ''
  mkdir -p $out/random $out/text
  for i in $(seq 1 64); do
    head -c 4194304 /dev/urandom > $out/random/$i
  done
  for i in $(seq 1 1000); do
    seq 1 500 > $out/text/$i
  done
''
