with import <nixpkgs> {};

stdenv.mkDerivation rec {
  name = "dolly";
  shellHook = ''
    unset CXX
  '';
  
  buildInputs = [
    jemalloc numactl llvm_5 clang_5 gcc ninja
  ];
}

