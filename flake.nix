{
  description = "cronstable, a cron daemon with a schedule model you can inspect";

  # nixpkgs is the only input: no flake-utils, so `nix flake check` resolves one
  # thing and the lock file stays a single entry.
  #
  # nixos-unstable rather than the 26.05 release, because three of this project's
  # floors land above what 26.05 carries: aiohttp 3.14.3 against its 3.13.5,
  # aiosmtplib 5.1.1 against its 5.1.0, and setuptools 83 against its 80.10.1.
  # unstable satisfies all three today. Move this to the release branch once one
  # catches up; nothing else here depends on which branch it is.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f:
        nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # The version comes from the top heading of HISTORY.md, which is the
      # release being prepared. Reading it here rather than hardcoding keeps
      # `nix run` from reporting a version this tree is not, and setuptools_scm
      # cannot help: the source Nix builds from is a store path with no git
      # history at all.
      version =
        let
          isHeading = line:
            builtins.isString line && builtins.match "## [0-9].*" line != null;
          headings =
            builtins.filter isHeading
              (builtins.split "\n" (builtins.readFile ./HISTORY.md));
        in
        builtins.head (builtins.match "## ([0-9.]+).*" (builtins.head headings));
    in
    {
      packages = forAllSystems (pkgs: rec {
        cronstable = pkgs.python3Packages.buildPythonApplication {
          pname = "cronstable";
          inherit version;
          src = ./.;
          pyproject = true;

          build-system = with pkgs.python3Packages; [
            setuptools
            setuptools-scm
          ];

          # setuptools_scm derives the version from git metadata, which a store
          # path does not carry, so it is told outright.
          env.SETUPTOOLS_SCM_PRETEND_VERSION = version;

          dependencies = with pkgs.python3Packages; [
            strictyaml
            aiohttp
            sentry-sdk
            aiosmtplib
            jinja2
            tzdata
            psutil
          ];

          # The test suite wants a writable HOME, network namespaces and a
          # handful of platform tools; `nix flake check` proves the package
          # BUILDS and imports, and CI runs the suite properly elsewhere.
          doCheck = false;
          pythonImportsCheck = [ "cronstable" ];

          meta = with pkgs.lib; {
            description = "Cron daemon with a schedule model you can inspect";
            homepage = "https://github.com/ptweezy/cronstable";
            license = licenses.mit;
            mainProgram = "cronstable";
            platforms = platforms.unix;
          };
        };

        default = cronstable;
      });

      apps = forAllSystems (pkgs: rec {
        cronstable = {
          type = "app";
          program = "${self.packages.${pkgs.system}.cronstable}/bin/cronstable";
        };
        default = cronstable;
      });

      checks = forAllSystems (pkgs: {
        build = self.packages.${pkgs.system}.cronstable;
      });
    };
}
