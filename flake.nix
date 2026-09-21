{
  description = "cronstable, a cron daemon with a schedule model you can inspect";

  # nixos-unstable supplies the newer Python dependencies this project needs.
  # PyPI releases can still arrive first; nix/python-packages.nix bridges
  # those gaps until nixpkgs catches up, preserving our dependency floors.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  # 26.05 is the final nixpkgs release supporting Intel macOS. Keep its native
  # toolchain while borrowing newer Python recipes when a dependency needs one.
  inputs.nixpkgs-intel-darwin.url = "github:NixOS/nixpkgs/nixpkgs-26.05-darwin";

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-intel-darwin,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f (
            if system == "x86_64-darwin" then
              nixpkgs-intel-darwin.legacyPackages.${system}
            else
              nixpkgs.legacyPackages.${system}
          )
        );

      # Share the build/runtime dependency lists with pip, Docker and binary
      # builds. Nixpkgs spells Python distribution names with hyphens.
      pyproject = builtins.fromTOML (builtins.readFile ./pyproject.toml);
      dependencyName =
        requirement:
        builtins.replaceStrings [ "_" ] [ "-" ] (
          builtins.head (builtins.match "([A-Za-z0-9_-]+).*" requirement)
        );
      dependenciesFrom =
        pkgs: packages: requirements:
        map (
          requirement:
          let
            name = dependencyName requirement;
            floor = builtins.head (builtins.match "[A-Za-z0-9_-]+>=([^,; ]+).*" requirement);
            selected = packages.${name};
          in
          assert pkgs.lib.assertMsg (pkgs.lib.versionAtLeast selected.version floor)
            "Nix dependency ${name} is below ${floor}; update nix/python-packages.nix";
          selected
        ) requirements;

      # The version comes from the top heading of HISTORY.md, which is the
      # release being prepared. Reading it here rather than hardcoding keeps
      # `nix run` from reporting a version this tree is not, and setuptools_scm
      # cannot help: the source Nix builds from is a store path with no git
      # history at all.
      version =
        let
          isHeading = line: builtins.isString line && builtins.match "## [0-9].*" line != null;
          headings = builtins.filter isHeading (builtins.split "\n" (builtins.readFile ./HISTORY.md));
        in
        builtins.head (builtins.match "## ([0-9.]+).*" (builtins.head headings));
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          currentPackages = import ./nix/python-packages.nix { inherit pkgs; };
          # The compatibility adapter is the only consumer of backported recipes.
          # Retirement steps: wiki/Contributing-and-Releasing.md, Intel Mac support.
          pythonPackages =
            if pkgs.stdenv.hostPlatform.system == "x86_64-darwin" then
              import ./nix/intel-darwin.nix {
                inherit pkgs nixpkgs pyproject;
                packages = currentPackages;
              }
            else
              currentPackages;
        in
        rec {
          cronstable = pkgs.python3Packages.buildPythonApplication {
            pname = "cronstable";
            inherit version;
            src = ./.;
            pyproject = true;

            build-system = dependenciesFrom pkgs pythonPackages pyproject.build-system.requires;

            # setuptools_scm derives the version from git metadata, which a store
            # path does not carry, so it is told outright.
            env.SETUPTOOLS_SCM_PRETEND_VERSION = version;

            dependencies = dependenciesFrom pkgs pythonPackages pyproject.project.dependencies;

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
        }
      );

      apps = forAllSystems (pkgs: rec {
        cronstable = {
          type = "app";
          program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.cronstable}/bin/cronstable";
        };
        default = cronstable;
      });

      checks = forAllSystems (pkgs: {
        build = self.packages.${pkgs.stdenv.hostPlatform.system}.cronstable;
      });
    };
}
