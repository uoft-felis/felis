with import <nixpkgs> {};

stdenv.mkDerivation rec {
  name = "dolly";
  shellHook = ''
     unset CC CXX
     export CC=clang
     export CXX=clang++
     export JAVA_HOME=/usr/java
     export LD_LIBRARY_PATH=~/.nix-profile/lib/ # To deal with lld
   '';
   
  buildInputs = [
    llvm_7 clang_7 lldb_7 lld_7 gtest gperftools python36 watchman nailgun
  ];
}
