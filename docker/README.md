# TileDB-VCF Docker Images

TileDB-VCF is a C++ library for efficient storage and retrieval of genomic variant-call data using [TileDB][].

## Quick reference

* :blue_book: [TileDB-VCF docs][vcf-docs]
* :package: [TileDB-VCF repo][vcf-repo]
* :whale: [Docker Hub][docker]

## Images

Pre-built images are available on [Docker Hub][docker].

### Variants

- [`tiledbvcf-cli`](https://hub.docker.com/r/tiledb/tiledbvcf-cli) for the command line interface (CLI)
- [`tiledbvcf-py`](https://hub.docker.com/r/tiledb/tiledbvcf-py) for the Python module

### Supported tags

* `latest`: latest stable release (*recommended*)
* `dev`: development version
* `v0.x.x` for a specific version


## Building

## Building locally

To build the Dockerfiles locally you need to clone the [TileDB-VCF][vcf-repo] repository from GitHub and run `docker build` from within the local repo's root directory.

```sh
git clone https://github.com/TileDB-Inc/TileDB-VCF.git
cd TileDB-VCF

# build the cli image
docker build -f docker/Dockerfile-cli .

# build the python library
docker build -f docker/Dockerfile-py .
```

## Usage

The following examples are meant to provide a quick overview of how to use these containers. See our [docs][vcf-docs] for more comprehensive introductions to TileDB-VCF's functionality.

Here, we're using a publicly accessible [TileDB-VCF dataset][vcf-samples-20] hosted on S3 that contains genome-wide variants for 20 synthetic samples. To pass in a local array or save exported files you will need to bind a local directory to the container's `/data` directory.

### CLI

You can use the `tiledbvcf-cli` image to run any of the CLI's [available commands][cli-api].

List all samples in the dataset:

```sh
docker run --rm tiledbvcf-cli list \
  --uri s3://tiledb-inc-demo-data/tiledb-arrays/2.0/vcf-samples-20
```

Create a table of all variants within a region of interest for sample `v2-WpXCYApL` and save the results in `./exported-vars.tsv`.

```sh
docker run --rm -v $PWD:/data tiledbvcf-cli export \
  --uri s3://tiledb-inc-demo-data/tiledb-arrays/2.0/vcf-samples-20 \
  -Ot --tsv-fields "CHR,POS,REF,S:GT" \
  --output-path exported-vars.tsv \
  --regions chr7:144000320-144008793 \
  --sample-names v2-WpXCYApL
```

### Python

You can use the `tiledbcf-py` container to execute an external script or launch an interactive Python session.

```
docker run -it --rm tiledbvcf-py
```

The following script performs the same query as above but returns a pandas `DataFrame`:

```py
import tiledbvcf

uri = "s3://tiledb-inc-demo-data/tiledb-arrays/2.0/vcf-samples-20"

# open the array in 'read' mode
ds = tiledbvcf.TileDBVCFDataset(uri, mode = "r")

ds.read(
  attrs=['sample_name', 'pos_start', 'pos_end', 'alleles'],
  regions=['chr7:144000320-144008793'],
  samples=['v2-WpXCYApL']
)
```

## Want to learn more?

Check out [TileDB-VCF's docs][vcf-docs] for installation instructions, detailed descriptions of the data format and algorithms, and in-depth tutorials.

<!-- links -->
[vcf-repo]: https://github.com/TileDB-Inc/TileDB-VCF
[docker]: https://hub.docker.com/u/tiledb
[tiledb]: https://tiledb.com
[vcf-docs]: https://docs.tiledb.com/genomics/
[cli-api]: https://docs.tiledb.com/genomics/apis/cli
[py-api]: https://docs.tiledb.com/genomics/apis/python
[vcf-samples-20]: https://console.tiledb.com/arrays/details/TileDB-Inc/vcf-samples-20-data
