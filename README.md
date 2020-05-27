<a href="https://tiledb.com"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin_@4x.png" alt="TileDB logo" width="400"></a>

[![Build Status](https://dev.azure.com/TileDB-Inc/CI/_apis/build/status/TileDB-VCF?branchName=master)](https://dev.azure.com/TileDB-Inc/CI/_build/latest?definitionId=8&branchName=master)

# :dna: TileDB-VCF

A C++ library for efficient storage and retrieval of genomic variant-call data using [TileDB][].

## Features

- Easily ingest large amounts of variant-call data at scale
- Supports ingesting single sample VCF and BCF files
- New samples are added *incrementally*, avoiding computationally expensive merging operations
- Allows for highly compressed storage using TileDB sparse arrays
- Efficient, parallelized queries of variant data stored locally or remotely on S3
- Export lossless VCF/BCF files or extract specific slices of a dataset

## What's Included?

- Command line interface (CLI)
- APIs for C, C++, Python, and Java
- Integrates with Spark, Dask, and AWS Batch

Check out our [docs][vcf] for installation and usage instructions:

|           |       :package:       |       :memo:       |
|----------:|:---------------------:|:------------------:|
|       CLI | [Install][inst-cli]   | [Usage][use-cli]   |
|    Python | [Install][inst-py]    | [Usage][use-py]    |
|     Spark | [Install][inst-spark] | [Usage][use-spark] |
| AWS Batch | [Install][inst-aws]   | [Usage][use-aws]   |

## Documentation

* Motivation and use case: https://docs.tiledb.com/genomics/
* Installation: https://docs.tiledb.com/genomics/installation
* Usage: https://docs.tiledb.com/genomics/usage
* Further reading: https://docs.tiledb.com/genomics/advanced
* TileDB-VCF developer resources: https://github.com/TileDB-Inc/TileDB-VCF/wiki

# Code of Conduct

All participants in TileDB spaces are expected to adhere to high standards of
professionalism in all interactions. This repository is governed by the
specific standards and reporting procedures detailed in depth in the
[TileDB core repository Code Of Conduct](
https://github.com/TileDB-Inc/TileDB/blob/dev/CODE_OF_CONDUCT.md).

<!-- links -->
[tiledb]: https://tiledb.com
[vcf]: https://docs.tiledb.com/genomics/

[inst-cli]: https://docs.tiledb.com/genomics/installation/standalone-tiledb-vcf
[inst-py]: https://docs.tiledb.com/genomics/installation/python
[inst-spark]: https://docs.tiledb.com/genomics/installation/spark
[inst-aws]: https://docs.tiledb.com/genomics/installation/aws-batch

[use-cli]: https://docs.tiledb.com/genomics/usage/cli
[use-py]: https://docs.tiledb.com/genomics/usage/python
[use-spark]: https://docs.tiledb.com/genomics/usage/spark
[use-aws]: https://docs.tiledb.com/genomics/usage/aws-batch
