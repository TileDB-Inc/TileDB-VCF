@echo on

rem Test libtiledbvcf assuming source code directory is .\TileDB-VCF\libtiledbvcf
rem and build directory is build-libtiledbvcf

make -j2 -C build-libtiledbvcf\libtiledbvcf tiledb_vcf_unit
$GITHUB_WORKSPACE\build-libtiledbvcf\libtiledbvcf\test\tiledb_vcf_unit

rem cli tests (require bcftools)
rem USAGE: run-cli-tests.sh <build-dir> <inputs-dir>
bash $GITHUB_WORKSPACE\TileDB-VCF\libtiledbvcf\test\run-cli-tests.sh build-libtiledbvcf TileDB-VCF\libtiledbvcf\test\inputs
