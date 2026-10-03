# Shared dependency updates while nixpkgs catches up with pyproject.toml.
# Keep upstream recipes and automatically prefer newer nixpkgs versions.
#
# No binary cache holds an override, so each CI run rebuilds it. Its upstream
# suite would rerun there too, and its timing tests fail on slow runners.
# Upstream tests its own releases, so overrides only build and import-check.
{ pkgs }:
let
  setuptools =
    let
      version = "84.0.0";
    in
    if pkgs.lib.versionAtLeast pkgs.python3Packages.setuptools.version version then
      pkgs.python3Packages.setuptools
    else
      pkgs.python3Packages.setuptools.overridePythonAttrs (old: {
        inherit version;
        src = pkgs.fetchFromGitHub {
          owner = "pypa";
          repo = "setuptools";
          tag = "v${version}";
          hash = "sha256-Kua7oN37yMbUS8K/cbEy1rTP00LlB4qHUqOX21TFTRQ=";
        };
        # The nixpkgs reproducible-wheel patch does not apply to this
        # release. Write the hardcoded version that the patch produces.
        patches = [ ];
        postPatch = ''
          echo "__version__ = '${version}'" > setuptools/version.py
        '';
        doCheck = false;
        meta = old.meta // {
          changelog = "https://setuptools.pypa.io/en/stable/history.html#v${
            builtins.replaceStrings [ "." ] [ "-" ] version
          }";
        };
      });

  # setuptools-scm 10.3.4 builds against and depends on this release. The
  # Intel macOS package set has no recipe for it, so this derivation is
  # self-contained.
  vcs-versioning =
    let
      version = "2.5.0";
      # The Intel macOS build tooling carries packaging 26.1, which a single
      # package cannot replace. vcs-versioning uses only long-standing
      # packaging APIs, so that package set relaxes its 26.2 floor.
      packagingLags = !pkgs.lib.versionAtLeast pkgs.python3Packages.packaging.version "26.2";
    in
    if
      pkgs.python3Packages ? vcs-versioning
      && pkgs.lib.versionAtLeast pkgs.python3Packages.vcs-versioning.version version
    then
      pkgs.python3Packages.vcs-versioning
    else
      pkgs.python3Packages.buildPythonPackage {
        pname = "vcs-versioning";
        inherit version;
        pyproject = true;
        src = pkgs.fetchPypi {
          pname = "vcs_versioning";
          inherit version;
          hash = "sha256-lWp5bjH4D+cU0hnW0d8Vpr8kfRD22FG/S5gnnQpC2lU=";
        };
        build-system = [ setuptools ];
        dependencies = [ pkgs.python3Packages.packaging ];
        pypaBuildFlags = pkgs.lib.optionals packagingLags [ "--skip-dependency-check" ];
        pythonRelaxDeps = pkgs.lib.optionals packagingLags [ "packaging" ];
        doCheck = false;
        pythonImportsCheck = [ "vcs_versioning" ];
        meta = {
          changelog = "https://github.com/pypa/setuptools-scm/releases/tag/vcs-versioning-v${version}";
          description = "Manage package versions from version control metadata";
          homepage = "https://github.com/pypa/setuptools-scm/tree/main/vcs-versioning";
          license = pkgs.lib.licenses.mit;
        };
      };
in
pkgs.python3Packages
// {
  inherit setuptools;
  # setuptools-scm propagates setuptools, so an overridden setuptools needs a
  # setuptools-scm built on it: two setuptools versions cannot share one build.
  setuptools-scm =
    let
      version = "10.3.4";
    in
    if
      setuptools == pkgs.python3Packages.setuptools && pkgs.lib.versionAtLeast pkgs.python3Packages.setuptools-scm.version version
    then
      pkgs.python3Packages.setuptools-scm
    else
      pkgs.python3Packages.setuptools-scm.overridePythonAttrs (old: {
        inherit version;
        src = pkgs.fetchPypi {
          pname = "setuptools_scm";
          inherit version;
          hash = "sha256-pp8ov8JFYIeBIF6RL6rkN8KyFldzr6TnuXnXdEemndI=";
        };
        build-system = [
          setuptools
          vcs-versioning
        ];
        dependencies = [
          pkgs.python3Packages.packaging
          setuptools
          vcs-versioning
        ];
        doCheck = false;
        meta = old.meta // {
          changelog = "https://github.com/pypa/setuptools-scm/blob/setuptools-scm-v${version}/setuptools-scm/CHANGELOG.md";
        };
      });
  sentry-sdk =
    let
      version = "2.71.0";
    in
    if pkgs.lib.versionAtLeast pkgs.python3Packages.sentry-sdk.version version then
      pkgs.python3Packages.sentry-sdk
    else
      pkgs.python3Packages.sentry-sdk.overridePythonAttrs (old: {
        inherit version;
        src = pkgs.fetchPypi {
          pname = "sentry_sdk";
          inherit version;
          hash = "sha256-e+snoi8GOW86BVEMPabqEOHHngLoWZk8J85P8HTiltY=";
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
