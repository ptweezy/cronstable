# Intel Mac compatibility boundary. Keep the final native nixpkgs toolchain,
# but never relax pyproject.toml's dependency/security floors to retain it.
# Remove this adapter with the nixpkgs-intel-darwin input when retiring Nix
# on Intel Macs; other platforms never evaluate these recipe backports.
{
  pkgs,
  nixpkgs,
  pyproject,
  packages,
}:
let
  # Current SMTP tests require the loop-factory hook absent in 26.05's plugin.
  recipeArguments.aiosmtplib.pytest-asyncio = pkgs.python3Packages.callPackage (
    nixpkgs + "/pkgs/development/python-modules/pytest-asyncio"
  ) { };
  backport =
    requirement:
    let
      parts = builtins.match "([A-Za-z0-9_-]+)>=([^,; ]+).*" requirement;
      name = builtins.replaceStrings [ "_" ] [ "-" ] (builtins.elemAt parts 0);
      floor = builtins.elemAt parts 1;
    in
    {
      inherit name;
      value =
        if pkgs.lib.versionAtLeast packages.${name}.version floor then
          packages.${name}
        else
          pkgs.python3Packages.callPackage (nixpkgs + "/pkgs/development/python-modules/${name}") (
            recipeArguments.${name} or { }
          );
    };
in
packages
// builtins.listToAttrs (
  map backport (pyproject.build-system.requires ++ pyproject.project.dependencies)
)
