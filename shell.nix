{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  buildInputs = [
    pkgs.gnumake
    pkgs.jq
    pkgs.jsonnet
    pkgs.nodejs
    pkgs.yarn
  ];  # join lists with ++

  nativeBuildInputs = [
    ~/setup/bash/git_shortcuts.sh
    ~/setup/bash/nix_shortcuts.sh
    ~/setup/bash/node_shortcuts.sh
  ];

  shellHook = ''
    activate-yarn-env

    alias test='jest --watch'
    function sqlite-query() {
      _input_file=$1
      shift
      if [ $# -eq 0 ]; then
        sqlite3 -init <(ts-node src/cli.ts --inputFile $_input_file --toSql) :memory:
      else
        sqlite3 -init <(ts-node src/cli.ts --inputFile $_input_file --toSql) :memory: "$*"
      fi
    }
  '' + ''
    echo-shortcuts ${__curPos.file}
  '';  # join strings with +
}
