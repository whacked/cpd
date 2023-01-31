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

  '' + ''
    echo-shortcuts ${__curPos.file}
  '';  # join strings with +
}
