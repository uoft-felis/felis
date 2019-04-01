with import <nixpkgs> {};

llvmPackages_7.libcxxStdenv.mkDerivation rec {
  name = "dolly";
  shellHook = ''
     export JAVA_HOME=/usr/java
     export LD_LIBRARY_PATH=~/.nix-profile/lib/ # To deal with lld
   '';

  buildInputs = [
    llvmPackages_7.libcxx llvmPackages_7.lld llvmPackages_7.lldb gtest gperftools python36 watchman
  ];
}
