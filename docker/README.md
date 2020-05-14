# TileDB-VCF docker images

This directory contains the Dockerfiles to build containers for various components of TileDB-VCF.

## `Dockerfile-cli`

This image builds the main TileDB-VCF library and CLI tool:
```bash
$ cd TileDB-VCF/
$ docker build -f docker/Dockerfile-cli -t tiledbvcf-cli .

# To test:
$ docker run --rm -it tiledbvcf-cli --version  # or '--help'
TileDB-VCF build
TileDB version 1.6.2
```

## `Dockerfile-py`

This image builds the main TileDB-VCF Python module and API:
```bash
$ cd TileDB-VCF/
$ docker build -f docker/Dockerfile-py -t tiledbvcf-py .

# To test:
$ docker run --rm -it --entrypoint python tiledbvcf-py
>>> import tiledbvcf
>>> # If no errors occur when importing, installation succeeded.
```


## `Dockerfile-dask-py`

This image builds a TileDB-VCF Python image suitable for use as the image for a dask worker:
```bash
$ cd TileDB-VCF/
$ docker build -f docker/Dockerfile-dask-py -t tiledbvcf-dask-py .
```
