# TileDB-VCF Docker Images

TileDB-VCF is a C++ library for efficient storage and retrieval of genomic variant-call data using [TileDB Embedded][tiledb].

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

## Parameters

* `AWS_EC2_METADATA_DISABLED` Disable the EC2 metadata service (default `true`). This avoids unnecessary API calls when querying S3-hosted arrays from non-EC2 clients. If you are on an EC2 instance this service can be enabled with `-e AWS_EC2_METADATA_DISABLED=false`.

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
docker run --rm tiledb/tiledbvcf-cli list \
  --uri s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20
```

Create a table of all variants within a region of interest for sample `v2-WpXCYApL`

```sh
docker run --rm -v $PWD:/data tiledb/tiledbvcf-cli export \
  --uri s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20 \
  -Ot --tsv-fields "CHR,POS,REF,S:GT"
  --regions chr7:144000320-144008793 \
  --sample-names v2-WpXCYApL
```

To avoid permission issues when creating TileDB-VCF datasets with Docker you need to provide the current user ID and group ID to the container:

```sh
docker run --rm \
  -v $PWD:/data \
  -u "$(id -u):$(id -g)" \
  tiledb/tiledbvcf-cli \
  create -u test-array
```

User and group IDs should also be specified when ingesting data:

```sh
# create a sample file with S3 URIs for 10 example BCF files
printf "s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G%i.bcf\n" $(seq 10) > samples.txt

docker run --rm \
  -v $PWD:/data \
  -u "$(id -u):$(id -g)" \
  tiledb/tiledbvcf-cli \
  store -u test-array -f samples.txt --scratch-mb 10 --verbose
```

### Python

You can use the `tiledbvcf-py` container to execute an external script or launch an interactive Python session.

```
docker run -it --rm tiledb/tiledbvcf-py
```

The following script performs the same query as above but returns a pandas `DataFrame`:

```py
import tiledbvcf

uri = "s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20"

# open the array in 'read' mode
ds = tiledbvcf.Dataset(uri, mode = "r")

ds.read(
  attrs=['sample_name', 'pos_start', 'pos_end', 'alleles'],
  regions=['chr7:144000320-144008793'],
  samples=['v2-WpXCYApL']
)

##     sample_name  pos_start    pos_end         alleles
## 0   v2-WpXCYApL  143999628  144000483  [G, <NON_REF>]
## 1   v2-WpXCYApL  144000484  144001253  [C, <NON_REF>]
## 2   v2-WpXCYApL  144001254  144001494  [G, <NON_REF>]
## 3   v2-WpXCYApL  144001495  144001735  [T, <NON_REF>]
## 4   v2-WpXCYApL  144001736  144001900  [C, <NON_REF>]
## ..          ...        ...        ...             ...
## 66  v2-WpXCYApL  144008445  144008467  [C, <NON_REF>]
## 67  v2-WpXCYApL  144008468  144008479  [T, <NON_REF>]
## 68  v2-WpXCYApL  144008480  144008690  [A, <NON_REF>]
## 69  v2-WpXCYApL  144008691  144008697  [A, <NON_REF>]
## 70  v2-WpXCYApL  144008698  144008846  [C, <NON_REF>]
##
## [71 rows x 4 columns]
```

## Want to learn more?

Check out [TileDB-VCF's docs][vcf-docs] for installation instructions, detailed descriptions of the data format and algorithms, and in-depth tutorials.

## License

This project is licensed under the MIT License - see the [LICENSE.md](https://github.com/TileDB-Inc/TileDB-VCF/blob/master/LICENSE) file for details.

<!-- links -->
[vcf-repo]: https://github.com/TileDB-Inc/TileDB-VCF
[docker]: https://hub.docker.com/u/tiledb
[tiledb]: https://github.com/TileDB-Inc/TileDB
[vcf-docs]: https://docs.tiledb.com/solutions/integrations/population-genomics
[cli-api]:  https://docs.tiledb.com/solutions/integrations/population-genomics/api-reference/cli
[py-api]:  https://docs.tiledb.com/solutions/integrations/population-genomics/api-reference/python
[vcf-samples-20]: https://console.tiledb.com/arrays/details/TileDB-Inc/vcf-samples-20-data
