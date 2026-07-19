# Fixture for the `hestia matrix` end-to-end test (tests/matrix_cli.rs):
# dependency-free checks so evaluation needs neither nixpkgs nor network.
{
  outputs =
    { ... }:
    {
      checks.x86_64-linux = rec {
        fast = derivation {
          name = "fast";
          system = "x86_64-linux";
          builder = "/bin/sh";
          args = [
            "-c"
            "echo fast > $out"
          ];
        };
        # Same derivation under a second name: must collapse to one job.
        fast-alias = fast;
        small-1 =
          derivation {
            name = "small-1";
            system = "x86_64-linux";
            builder = "/bin/sh";
            args = [
              "-c"
              "echo 1 > $out"
            ];
          }
          // {
            meta.hestia.group = "small";
          };
        small-2 =
          derivation {
            name = "small-2";
            system = "x86_64-linux";
            builder = "/bin/sh";
            args = [
              "-c"
              "echo 2 > $out"
            ];
          }
          // {
            meta.hestia.group = "small";
          };
      };
    };
}
