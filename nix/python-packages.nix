# Shared dependency updates while nixpkgs catches up with pyproject.toml.
# Keep upstream recipes and automatically prefer newer nixpkgs versions.
#
# No binary cache holds an override, so each CI run rebuilds it. Its upstream
# suite would rerun there too, and its timing tests fail on slow runners.
# Upstream tests its own releases, so overrides only build and import-check.
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
        doCheck = false;
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
        doCheck = false;
        meta = old.meta // {
          changelog = "https://github.com/python/tzdata/blob/${version}/NEWS.md";
        };
      });
}
