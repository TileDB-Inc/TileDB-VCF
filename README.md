<a href="https://tiledb.com"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin@4x.png" alt="TileDB logo" width="400"></a>

# TileDB-VCF

TileDB-VCF is a library for efficient storage and retrieval of genomics variant-call data. With it, you can easily ingest large amounts of variant-call data from the VCF (or BCF) format into a 2D sparse TileDB array that allows for highly compressed storage and efficient, parallelized queries on the variant data. The motivation and idea behind storing VCF data in a 2D sparse array is described in our [Genomics use case docs](https://docs.tiledb.com/main/use-cases/genomics).

## Quick Links

* Motivation and use case: https://docs.tiledb.com/main/use-cases/genomics
* Installation: https://docs.tiledb.com/developer/tiledbvcf/installation
* Usage: https://docs.tiledb.com/developer/tiledbvcf/usage
* Further reading: https://docs.tiledb.com/developer/tiledbvcf/advanced

## Development

## Regenerating test arrays

If the format changes, regenerate the unit test arrays:
```bash
rm -r libtiledbvcf/test/inputs/arrays*

tiledbvcf create -u libtiledbvcf/test/inputs/arrays/ingested_2samples
tiledbvcf register -u libtiledbvcf/test/inputs/arrays/ingested_2samples libtiledbvcf/test/inputs/small2.bcf libtiledbvcf/test/inputs/small.bcf
tiledbvcf store -u libtiledbvcf/test/inputs/arrays/ingested_2samples libtiledbvcf/test/inputs/small2.bcf libtiledbvcf/test/inputs/small.bcf

tiledbvcf create -u libtiledbvcf/test/inputs/arrays/ingested_2samples_GT_DP_PL/ -a fmt_GT,fmt_DP,fmt_PL
tiledbvcf register -u libtiledbvcf/test/inputs/arrays/ingested_2samples_GT_DP_PL/ libtiledbvcf/test/inputs/small2.bcf libtiledbvcf/test/inputs/small.bcf
tiledbvcf store -u libtiledbvcf/test/inputs/arrays/ingested_2samples_GT_DP_PL/ libtiledbvcf/test/inputs/small2.bcf libtiledbvcf/test/inputs/small.bcf
```
