#!/bin/sh

patchelf --set-rpath "" $1
patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 $1
