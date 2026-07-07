{ pkgs ? (
    let
      inherit (builtins) fetchTree fromJSON readFile;
      inherit ((fromJSON (readFile ./flake.lock)).nodes) nixpkgs gomod2nix;
    in
    import (fetchTree nixpkgs.locked) {
      overlays = [
        (import "${fetchTree gomod2nix.locked}/overlay.nix")
      ];
    }
  )
, buildGoApplication ? pkgs.buildGoApplication
}:

buildGoApplication {
  pname = "cpd";
  version = "20260424.5.d7c3631";
  pwd = ./.;
  src = ./.;
  modules = ./gomod2nix.toml;
  CGO_ENABLED = 0;
  tags = [ "cue" ];
  flags = [
    "-mod=readonly"
  ];
  doCheck = false;
  postInstall = ''
    mv $out/bin/yamdb $out/bin/cpd

    # Ship the VisiData loader beside the binary. Layout: <prefix>/bin/cpd and
    # <prefix>/share/visidata/vdcpd.py, so vdcpd finds cpd install-relative
    # (../../bin/cpd) and a later make/sdflow install target can reuse it.
    install -Dm644 vdcpd.py $out/share/visidata/vdcpd.py
    install -Dm644 visidatarc.example $out/share/visidata/visidatarc.example
  '';
}
