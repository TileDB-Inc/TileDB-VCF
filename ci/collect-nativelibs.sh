#!/bin/bash
mv ../libraries/* .

mkdir lib

for arch in $(ls | grep .tar.gz)
do
tar -xf $arch
done

# OSX
mv *.dylib lib

# Linux
mv *.so* lib
