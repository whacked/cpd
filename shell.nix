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
  goEnv = mkGoEnv { pwd = ./.; go = pkgs.go_1_23; };
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
    pkgs.mdsh
  ];  # join lists with ++

  nativeBuildInputs = [
    ~/setup/bash/git_shortcuts.sh
    ~/setup/bash/nix_shortcuts.nix.sh
    ~/setup/bash/node_shortcuts.sh
  ];

  name = "yamdb-go";

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

    # ensure-goenv: Sets up a isolated Go development environment.
    #
    # Usage: ensure-goenv <env_name>
    #   <env_name>: A unique name for this Go environment. This will be used
    #               to create a dedicated directory for GOPATH, GOBIN, etc.
    #
    # This function performs the following steps:
    # 1. Determines the user's cache directory (XDG_CACHE_HOME or ~/.cache).
    # 2. Sets GOPATH, GOBIN, and updates PATH to point to a new, isolated
    #    Go environment directory within the cache.
    # 3. Checks if 'gopls' is already available in the updated PATH.
    # 4. If 'gopls' is not found, it creates the environment directory
    #    and proceeds to install 'gopls' and 'staticcheck' into the new GOBIN.
    # 5. Provides informative messages and bails out if critical steps (like
    #    directory creation) fail.
    #
    # Example:
    #   ensure-goenv myproject-dev
    #   # Now, gopls and staticcheck are available in $GOPATH/bin for this session.
    ensure-goenv() {  # get or create a go env for vs code
        local env_name="$1"
        local go_env_base_dir="$USERCACHE/''${env_name}-goenv"
        local go_bin_dir="''${go_env_base_dir}/bin"

        if [[ -z "$env_name" ]]; then
            echo "Error: Missing environment name. Usage: ensure-goenv <env_name>"
            return 1
        fi

        echo "Setting up Go environment for: ''${env_name}"
        echo "  GOPATH will be: ''${go_env_base_dir}"
        echo "  GOBIN will be:  ''${go_bin_dir}"

        # Set GOPATH and GOBIN for the current shell session
        export GOPATH="''${go_env_base_dir}"
        export GOBIN="''${go_bin_dir}"

        # Prepend the new GOBIN to the PATH for the current shell session
        # This ensures that tools installed into GOBIN are found.
        export PATH="''${GOBIN}:''${PATH}"

        # Check if gopls is in the PATH.
        if ! command -v gopls &> /dev/null; then
            echo "gopls not found in PATH. Attempting to install gopls and staticcheck..."

            # Create the environment directory if it doesn't exist
            if ! mkdir -p "''${go_bin_dir}"; then
                echo "Error: Failed to create directory ''${go_bin_dir}. Aborting."
                return 1
            fi
            echo "Created Go environment directory: ''${go_env_base_dir}"

            # Install gopls
            echo "Installing gopls (golang.org/x/tools/gopls@latest)..."
            if ! go install golang.org/x/tools/gopls@latest; then
                echo "Error: Failed to install gopls. Please ensure Go is correctly installed and configured."
                return 1
            fi
            echo "gopls installed successfully."

            # Install staticcheck
            echo "Installing staticcheck (honnef.co/go/tools/cmd/staticcheck@latest)..."
            if ! go install honnef.co/go/tools/cmd/staticcheck@latest; then
                echo "Error: Failed to install staticcheck. Please ensure Go is correctly installed and configured."
                return 1
            fi
            echo "staticcheck installed successfully."
        else
            echo "gopls already found in PATH. Skipping installation."
        fi

        echo "Go environment ''${env_name} is ready."
        echo "Current GOPATH: ''${GOPATH}"
        echo "Current GOBIN:  ''${GOBIN}"
    }

    ensure-goenv $name

    function view() {
      # This searches for lines starting with // followed by optional whitespace,
      # or for 'func' declarations, and then tries to show context.
      # It only processes files ending with .go
      grep -E -A 2 -r --include='*.go' '^\s*//.*|func ' .
    }

    alias cli="tsx $_CLI_PATH"
  '' + ''
    export PATH=$PWD:$PATH
    export GOPATH=$PWD/.devenv
    export GOBIN=$GOPATH/bin
    echo-shortcuts ${__curPos.file}
  '';  # join strings with +
}
