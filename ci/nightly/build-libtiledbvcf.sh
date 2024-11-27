#!/bin/bash
set -ex

# Build libtiledbvcf assuming source code directory is ./TileDB-VCF/libtiledbvcf

cmake -S TileDB-VCF/libtiledbvcf -B build-libtiledbvcf \
  -D CMAKE_BUILD_TYPE=Release \
  -D CMAKE_INSTALL_PREFIX:PATH=$GITHUB_WORKSPACE/install/ \
  -D OVERRIDE_INSTALL_PREFIX=OFF \
  -D TILEDB_WERROR=OFF

cmake --build build-libtiledbvcf -j2 --config Release

cmake --install build-libtiledbvcf 

./install/tiledbvcf/bin/tiledbvcf version
