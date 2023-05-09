---
title: CLI
---


## Create a new dataset

::: {.callout-note}
The files used in the following examples can be obtained [here](https://github.com/TileDB-Inc/TileDB-Examples/tree/master/genomics/data).
:::

The first step is to **create** an empty dataset. Let's save the dataset in a new array called `small_dataset`:

```bash
tiledbvcf create --uri small_dataset
```

## Store samples

### Ingest from local storage

We'll start with a small example using 3 synthetic VCF files, assuming they are locally available in a folder `data/vcfs`:

```bash
tree data
## data
## ├── gene-promoters-hg38.bed
## ├── s3-bcf-samples.txt
## └── vcfs
##     ├── G1.vcf.gz
##     ├── G1.vcf.gz.csi
##     ├── G2.vcf.gz
##     ├── G2.vcf.gz.csi
##     ├── G3.vcf.gz
##     └── G3.vcf.gz.csi
##
## 1 directory, 8 files
```

Index files are required for ingestion. If your VCF/BCF files have not been indexed you can use [`bcftools`](https://samtools.github.io/bcftools/bcftools.html) to do so:

```bash
for f in data/vcfs/*.vcf.gz; do bcftools index -c $f; done
```

We can ingest these files into `small_dataset` as follows:

```bash
tiledbvcf store --uri small_dataset data/vcfs/G*.vcf.gz
```

That's it! Let's verify everything went okay using the `stat` command to provide high-level statistics about our dataset including the number of samples it contains and the variant attributes it includes.

```bash
tiledbvcf stat --uri small_dataset
## Statistics for dataset 'small_dataset':
## - Version: 4
## - Tile capacity: 10,000
## - Anchor gap: 1,000
## - Number of registered samples: 3
## - Extracted attributes: none
```

At this point you have successfully created and populated a TileDB VCF dataset using data stored locally on your machine. Next we'll look at the more common scenario of working with files stored on a cloud object store. 

### Ingest from S3

TileDB Embedded's native cloud features make it possible to ingest samples directly from remote locations. Here, we'll ingest the following samples located on AWS S3:

```bash
cat data/s3-bcf-samples.txt
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G4.bcf
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G5.bcf
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G6.bcf
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G7.bcf
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G8.bcf
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G9.bcf
## s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G10.bcf
```

::: {.callout-note}
Samples in this second batch are stored as `BCF` files which are also supported by TileDB-VCF.
:::

This process is identical to the steps perfo_r_med above, the only changes needed to our code involve setting `--scratch-mb` to allocate some temporary space for downloading the files and providing the URLs for the remote files. In this case, we'll simply pass the `s3-bcf-samples.txt` file, which includes a list of the BCF files we want to ingest.

::: {.callout-note}
When ingesting samples from S3, you must configure enough scratch space to hold at least 20 samples. In general, you need 2 × the sample dimension's `sample_bactch_size` \(which by default is 10\). You can read more about the data model [here](../data-model.md).
:::

You can add the `--verbose` flag to print out more information during the `store` phase.

```bash
tiledbvcf store \
  --uri small_dataset \
  --samples-file data/s3-bcf-samples.txt \
  --verbose
  
## Initialization completed in 3.17565 sec.
## ...
## Done. Ingested 1,391 records (+ 69,548 anchors) from 7 samples in 10.6751
## seconds.
```

Consolidating and vacuuming fragment metadata and commits are recommended after creating a new dataset or adding several new samples to an existing dataset.

```bash
tiledbvcf utils consolidate fragment_meta --uri small_dataset
tiledbvcf utils consolidate commits --uri small_dataset
tiledbvcf utils vacuum fragment_meta --uri small_dataset
tiledbvcf utils vacuum commits --uri small_dataset
```

## Incremental Updates

A key advantage to using TileDB as a data store for genomic variant data is the ability to efficiently add new samples as they become available. The dataset creation command should be called _once_. Then you can invoke the store command multiple commands. 

::: {.callout-note}
The `store` command is _thread-_ and _process-safe_, even on the cloud. That means it can be invoked **in parallel**, arbitrarily scaling out the ingestion of massive datasets.
:::

Suppose we run the store for the first 3 local samples, followed by the store command for the 7 samples stored on the S3. If we run the `stat` command, we can verify that our dataset now includes 10 samples.

```bash
tiledbvcf stat --uri small_dataset
## Statistics for dataset 'small_dataset':
## - Version: 4
## - Tile capacity: 10,000
## - Anchor gap: 1,000
## - Number of registered samples: 10
## - Extracted attributes: none
```

Because TileDB is designed to be updatable, the store process happens efficiently and without ever touching any previously or concurrently ingested data, avoiding computationally expensive operations like regenerating combined VCF files.