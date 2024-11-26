#!/bin/bash
set -ex

# Test libtiledbvcf assuming source code directory is ./TileDB-VCF/libtiledbvcf
# and build directory is build-libtiledbvcf

make -j2 -C build-libtiledbvcf tiledb_vcf_unit
./build-libtiledbvcf/test/tiledb_vcf_unit

# cli tests (require bcftools)
# USAGE: run-cli-tests.sh <build-dir> <inputs-dir>
TileDB-VCF/libtiledbvcf/test/run-cli-tests.sh build-libtiledbvcf TileDB-VCF/libtiledbvcf/test/inputs
