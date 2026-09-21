# Intel Mac compatibility boundary. Keep the final native nixpkgs toolchain,
# but never relax pyproject.toml's dependency/security floors to retain it.
# Remove this adapter, its test patch and the nixpkgs-intel-darwin input when
# retiring Intel Nix; other platforms never evaluate these recipe backports.
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
      backported = pkgs.python3Packages.callPackage (
        nixpkgs + "/pkgs/development/python-modules/${name}"
      ) (recipeArguments.${name} or { });
    in
    {
      inherit name;
      value =
        if pkgs.lib.versionAtLeast packages.${name}.version floor then
          packages.${name}
        else if name == "aiosmtplib" then
          backported.overridePythonAttrs (old: {
            # Test-only: sandboxed DNS stalls otherwise exceed SMTP deadlines.
            # Remove with this recipe backport when upstream handles it.
            patches = (old.patches or [ ]) ++ [ ./intel-darwin-aiosmtplib.patch ];
          })
        else
          backported;
    };
in
packages
// builtins.listToAttrs (
  map backport (pyproject.build-system.requires ++ pyproject.project.dependencies)
)
