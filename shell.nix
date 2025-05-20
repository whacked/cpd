{ pkgs ? (
    let
      inherit (builtins) fetchTree fromJSON readFile;
      inherit ((fromJSON (readFile ./../jdxd/flake.lock)).nodes) nixpkgs gomod2nix;
    in
    import (fetchTree nixpkgs.locked) {
      overlays = [
        (import "${fetchTree gomod2nix.locked}/overlay.nix")
      ];
    }
  )
, mkGoEnv ? pkgs.mkGoEnv
, gomod2nix ? pkgs.gomod2nix
}:

let
  goEnv = mkGoEnv { pwd = ./.; };
  go-jsonschema = pkgs.stdenv.mkDerivation {
    name = "go-jsonschema";
    src = pkgs.fetchurl {
      url = "https://github.com/omissis/go-jsonschema/releases/download/v0.15.0/go-jsonschema_Linux_x86_64.tar.gz";
      sha256 = "diR8EUGrEcVyhW5kAyDyHluoWRnj3lUlNL2BbhUjFS4=";
    };
    dontUnpack = true;
    installPhase = ''
      mkdir -p $out/bin
      tar -xzf $src -C $out/bin
    '';
    buildInputs = [ pkgs.unzip ];
  };
in
pkgs.mkShell {
  buildInputs = [
    pkgs.gnumake
    pkgs.jq
    # nokogiri build shenanigans
    # pkgs.jsonnet
    pkgs.nodejs
    pkgs.yarn
    goEnv
    gomod2nix
    go-jsonschema
    pkgs.check-jsonschema
  ];  # join lists with ++

  nativeBuildInputs = [
    ~/setup/bash/git_shortcuts.sh
    ~/setup/bash/nix_shortcuts.nix.sh
    ~/setup/bash/node_shortcuts.sh
  ];

  shellHook = ''
    activate-node-env

    alias test='jest --watch'

    _CLI_PATH=$PWD/src/cli.ts
    function sqlite-query() {
      _input_file=$1
      shift
      if [ $# -eq 0 ]; then
        sqlite3 -init <(tsx $_CLI_PATH --inputFile $_input_file --toSql) :memory:
      else
        sqlite3 -init <(tsx $_CLI_PATH --inputFile $_input_file --toSql) :memory: "$*"
      fi
    }

    alias cli="tsx $_CLI_PATH"
  '' + ''
    export PATH=$PWD:$PATH
    echo-shortcuts ${__curPos.file}
  '';  # join strings with +
}
