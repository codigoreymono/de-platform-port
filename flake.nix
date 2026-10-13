
{
  description = "NYC 311 Data Engineering Platform";

  inputs = {
    nixpkgs.url = "github:cachix/devenv-nixpkgs/rolling";
    devenv.url = "github:cachix/devenv";
  };

  outputs =
    inputs@{ nixpkgs, devenv, ... }:
    let
      system = "x86_64-linux";

      pkgs = import nixpkgs {
        inherit system;

        config.allowUnfreePredicate =
          pkg: nixpkgs.lib.getName pkg == "terraform";
      };
    in
    {
      devShells.${system}.default = devenv.lib.mkShell {
        inherit inputs pkgs;

        modules = [
          ./devenv.nix
        ];
      };
    };
}
