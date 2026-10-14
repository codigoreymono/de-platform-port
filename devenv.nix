
{ pkgs, ... }:

{
  # Python runtime
  languages.python = {
    enable = true;
    package = pkgs.python312;
  };

  # Development tools
  packages = with pkgs; [
    uv
    ruff
    duckdb

    # AWS tooling
    awscli2
    terraform

    # Utilities
    jq
  ];

  # Native libraries for Python packages
  env.LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
    pkgs.stdenv.cc.cc.lib
    pkgs.zlib
  ];
}
