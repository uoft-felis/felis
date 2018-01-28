with import <nixpkgs> {};

myEnvFun {
  name = "dolly";
  buildInputs = [
    jemalloc numactl llvm_5 clang_5 gcc
  ];
}

