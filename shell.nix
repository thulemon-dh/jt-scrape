let
  pkgs = import <nixpkgs> {};
in pkgs.mkShell {
  packages = with pkgs; [
      python3
      uv
      watchexec
  ];
  shellHook = with pkgs; ''
      export LD_LIBRARY_PATH=${stdenv.cc.cc.lib}/lib/
      source .venv/bin/activate
  '';
}
