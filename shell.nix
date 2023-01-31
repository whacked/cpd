{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  buildInputs = [
    pkgs.yarn
    pkgs.nodejs
  ];  # join lists with ++

  nativeBuildInputs = [
    ~/setup/bash/git_shortcuts.sh
    ~/setup/bash/nix_shortcuts.sh
    ~/setup/bash/node_shortcuts.sh
  ];

  shellHook = ''
    activate-yarn-env

  '' + ''
    echo-shortcuts ${__curPos.file}
  '';  # join strings with +
}
