# TileDB-VCF Docker Images

TileDB-VCF is a C++ library for efficient storage and retrieval of genomic variant-call data using [TileDB][].

Pre-built images are available on [Docker Hub](https://hub.docker.com/u/tiledb) with the following tags:

* `latest`: latest stable release (*recommended*)
* `dev`: development version

## Building

You can also build the Dockerfiles locally by cloning the TileDB-VCF repository from GitHub and running the following commands:

```sh
git clone https://github.com/TileDB-Inc/TileDB-VCF.git
cd TileDB-VCF

# build the cli image
docker build -f docker/Dockerfile-cli .

# build the python library
docker build -f docker/Dockerfile-py .
```

## Usage

In these examples we'll use a publicly accessible TileDB-VCF dataset hosted on S3. TileDB provides native support for a number of cloud storage backends, so you can work with this data directly, without having to download it first.

### CLI

Create a table of all variants within one or more regions of interest for 2 samples:

```sh
docker run --rm tiledbvcf-cli:dev export \
  --uri s3://tiledb-inc-demo-data/tiledb-arrays/2.0/vcf-samples-20 \
  -Ot --tsv-fields CHR,POS,REF,S:GT \
  --sample-names v2-usVwJUmo,v2-WpXCYApL \
  --regions chr1:1-50000
```

### Python

You can use the `tiledbcf-py` image to execute an external script or launch an interactive Python session.

```
docker run -it --rm tiledbvcf-py:dev
```

Performing the same query in python returns a pandas `DataFrame`

```py
import tiledbvcf

uri = "s3://tiledb-inc-demo-data/tiledb-arrays/2.0/vcf-samples-20"

# open the array in 'read' mode
ds = tiledbvcf.TileDBVCFDataset(uri, mode = "r")

ds.read(
  attrs=['sample_name', 'pos_start', 'pos_end', 'alleles'],
  regions=['chr1:1-50000'],
  samples=['v2-usVwJUmo', 'v2-WpXCYApL']
)
```

## Want to learn more?

Check out [TileDB-VCF's docs][vcf] for installation instructions, in-depth descriptions of the data format and algorithms, more examples and in-depth tutorials.

<!-- links -->
[tiledb]: https://tiledb.com
[vcf]: https://docs.tiledb.com/genomics/
