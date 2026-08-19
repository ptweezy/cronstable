{
  description = "cronstable, a cron daemon with a schedule model you can inspect";

  # nixpkgs is the only input: no flake-utils, so `nix flake check` resolves one
  # thing and the lock file stays a single entry. The stable branch rather than
  # unstable, because this is a packaging target and not a development shell.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

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

          # pyproject.toml asks for setuptools 83, the version the published
          # wheel and sdist are built with; nixpkgs carries 80. Nothing in this
          # build needs the difference, and pinning a second setuptools into
          # the closure to satisfy a build-time floor costs more than it
          # settles, so the floor is relaxed for the Nix build alone.
          postPatch = ''
            substituteInPlace pyproject.toml \
              --replace-fail "setuptools>=83" "setuptools>=80"
          '';

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
