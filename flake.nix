{
  description = "Ratatosk - standalone Redis-compatible data server packaged with Nix";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      crane,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        lib = pkgs.lib;

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "clippy"
            "rust-analyzer"
            "rust-src"
            "rustfmt"
          ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = lib.cleanSourceWith {
          src = ./.;
          filter = craneLib.filterCargoSources;
        };

        commonArgs = {
          inherit src;
          pname = "ratatosk";
          version = "0.1.0";
          strictDeps = true;
          cargoExtraArgs = "-p ratatosk-server --bin ratatosk";

          nativeBuildInputs = with pkgs; [
            pkg-config
          ];
        };

        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        mkRatatoskPackage =
          {
            pname,
            cargoExtraArgs ? commonArgs.cargoExtraArgs,
            RATATOSK_NIX_PROFILE ? null,
            extraMeta ? { },
          }:
          craneLib.buildPackage (
            commonArgs
            // {
              inherit cargoArtifacts pname cargoExtraArgs;

              inherit RATATOSK_NIX_PROFILE;

              postInstall = ''
                mkdir -p $out/share/doc/ratatosk $out/share/examples/ratatosk
                cp ${./README.md} $out/share/doc/ratatosk/README.md
                cp -r ${./docs} $out/share/doc/ratatosk/docs
                cp ${./ratatosk.conf} $out/share/examples/ratatosk/ratatosk.conf
                chmod -R u+w $out/share/doc/ratatosk $out/share/examples/ratatosk
              '';

              meta = {
                description = "Standalone Redis-compatible in-memory data server";
                license = lib.licenses.gpl3Plus;
                mainProgram = "ratatosk";
                platforms = lib.platforms.unix;
              }
              // extraMeta;
            }
          );

        ratatosk = mkRatatoskPackage {
          pname = "ratatosk";
          RATATOSK_NIX_PROFILE = "default";
        };

        nixFormatter = pkgs.writeShellApplication {
          name = "ratatosk-nixfmt";
          runtimeInputs = [ pkgs.nixfmt ];
          text = ''
            exec ${lib.getExe pkgs.nixfmt} "$@"
          '';
        };
      in
      {
        packages = {
          inherit ratatosk;
          default = ratatosk;
        };

        apps = {
          default = {
            type = "app";
            program = "${ratatosk}/bin/ratatosk";
          };

          ratatosk = {
            type = "app";
            program = "${ratatosk}/bin/ratatosk";
          };
        };

        formatter = nixFormatter;

        checks = {
          inherit ratatosk;

          ratatosk-check = craneLib.cargoCheck (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoExtraArgs = "--workspace --all-targets";
            }
          );

          ratatosk-clippy = craneLib.cargoClippy (
            commonArgs
            // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--workspace --all-targets -- -D warnings";
            }
          );

          ratatosk-fmt = craneLib.cargoFmt {
            inherit src;
          };

          ratatosk-nextest = craneLib.cargoNextest (
            commonArgs
            // {
              inherit cargoArtifacts;
              partitions = 1;
              partitionType = "count";
              cargoNextestExtraArgs = "--workspace";
            }
          );
        };

        devShells.default = pkgs.mkShell {
          packages = (
            with pkgs;
            [
              cargo-audit
              cargo-deny
              cargo-edit
              cargo-nextest
              cargo-watch
              just
              nixd
              nixfmt
              pkg-config
              rustToolchain
            ]
          );

          RUST_BACKTRACE = "1";
          RUST_LOG = "debug";

          shellHook = ''
            echo "Ratatosk dev shell"
            echo "  cargo check --workspace --quiet"
            echo "  nix build .#ratatosk"
            echo "  nix run .#ratatosk"
          '';
        };
      }
    );
}
