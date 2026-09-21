# Shared dependency updates while nixpkgs catches up with pyproject.toml.
# Retain upstream recipes/checks and automatically prefer newer nixpkgs versions.
{ pkgs }:
pkgs.python3Packages
// {
  sentry-sdk =
    let
      version = "2.69.2";
    in
    if pkgs.lib.versionAtLeast pkgs.python3Packages.sentry-sdk.version version then
      pkgs.python3Packages.sentry-sdk
    else
      pkgs.python3Packages.sentry-sdk.overridePythonAttrs (old: {
        inherit version;
        src = pkgs.fetchPypi {
          pname = "sentry_sdk";
          inherit version;
          hash = "sha256-tNiRWlJuYmsLFKKSWQdVS3ItO44+N4FXD8NXkXEq8yM=";
        };
        # 2.69.2 assumes a concurrent request finishes in 100 ms.
        # Wait for the asserted state with a deadline on loaded CI
        # hosts. Test-only patch; remove when upstream fixes the race.
        patches = (old.patches or [ ]) ++ [ ./sentry-async-test.patch ];
        meta = old.meta // {
          changelog = "https://github.com/getsentry/sentry-python/blob/${version}/CHANGELOG.md";
        };
      });
  tzdata =
    let
      version = "2026.4";
    in
    if pkgs.lib.versionAtLeast pkgs.python3Packages.tzdata.version version then
      pkgs.python3Packages.tzdata
    else
      pkgs.python3Packages.tzdata.overridePythonAttrs (old: {
        inherit version;
        src = pkgs.fetchPypi {
          pname = "tzdata";
          inherit version;
          hash = "sha256-8bi9Nl2NIQxVNT9Nf41thWHAulDXBLcA0ZWpQku6DXk=";
        };
        meta = old.meta // {
          changelog = "https://github.com/python/tzdata/blob/${version}/NEWS.md";
        };
      });
}
