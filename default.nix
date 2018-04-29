with import <nixpkgs> {};

stdenv.mkDerivation rec {

  cppnetlib = callPackage (

{ stdenv, fetchurl, cmake, boost, openssl, asio }:

stdenv.mkDerivation rec {
  name = "cpp-netlib-${version}";
  version = "0.12.0";

  src = fetchurl {
    url = "http://downloads.cpp-netlib.org/${version}/${name}-final.tar.bz2";
    sha256 = "0h7gyrbr3madycnj8rl8k1jzk2hd8np2k5ad9mijlh0fizzzk3h8";
  };

  buildInputs = [ cmake boost openssl ];

  # This can be removed when updating to 0.13, see https://github.com/cpp-netlib/cpp-netlib/issues/629
  propagatedBuildInputs = [ asio ];

  cmakeFlags = [];

  enableParallelBuilding = true;

  meta = with stdenv.lib; {
    description =
      "Collection of open-source libraries for high level network programming";
    homepage    = http://cpp-netlib.org;
    license     = licenses.boost;
    platforms   = platforms.all;
  };
}

  ){};

  name = "dolly";
  shellHook = ''
     unset CC CXX
   '';
   
  buildInputs = [
    jemalloc numactl llvm_5 clang_5 ninja cppnetlib
  ];
}
