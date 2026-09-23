# Intel Mac compatibility boundary. Keep the final native nixpkgs toolchain,
# but never relax pyproject.toml's dependency/security floors to retain it.
# Remove this adapter and the nixpkgs-intel-darwin input when retiring Intel
# Nix; other platforms never evaluate these recipe backports.
{
  pkgs,
  nixpkgs,
  pyproject,
  packages,
}:
let
  backport =
    requirement:
    let
      parts = builtins.match "([A-Za-z0-9_-]+)>=([^,; ]+).*" requirement;
      name = builtins.replaceStrings [ "_" ] [ "-" ] (builtins.elemAt parts 0);
      floor = builtins.elemAt parts 1;
      recipe = nixpkgs + "/pkgs/development/python-modules/${name}";
      # Build and import-check only, like the shared overrides: no cache
      # holds a backport, and its suite would rerun on every Intel build.
      backported = (pkgs.python3Packages.callPackage recipe { }).overridePythonAttrs {
        doCheck = false;
      };
    in
    {
      inherit name;
      value =
        if pkgs.lib.versionAtLeast packages.${name}.version floor then
          packages.${name}
        else
          backported;
    };
in
packages
// builtins.listToAttrs (
  map backport (pyproject.build-system.requires ++ pyproject.project.dependencies)
)
