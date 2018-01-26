with import <nixpkgs> {};

myEnvFun {
  name = "dolly";
  buildInputs = [
    jemalloc numactl clang_5 gcc
  ];
}

