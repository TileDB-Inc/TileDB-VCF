---
title: Python
---

## Python Ingestion

Similar to TileDB-VCF's command-line interface \(CLI\), `tiledbvcf` supports ingesting VCF \(or BCF\) files into TileDB, either when creating a new dataset _or_ updating an existing dataset with additional samples. See the [CLI Usage](./cli.md) for a more detailed description of the ingestion process. Here, we'll only focus on the mechanics of ingestion from Python.

The text file `data/s3-bcf-samples.txt` contains a list of S3 URIs pointing to 7 BCF files from the same cohort.

```python
with open("data/s3-bcf-samples.txt") as f:
    sample_uris = [l.rstrip("\n") for l in f.readlines()]
sample_uris
## ['s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G4.bcf',
##   's3://tiledb-inc-demo-data/examples/notebooks/vcfs/G5.bcf',
##   's3://tiledb-inc-demo-data/examples/notebooks/vcfs/G6.bcf',
##   's3://tiledb-inc-demo-data/examples/notebooks/vcfs/G7.bcf',
##   's3://tiledb-inc-demo-data/examples/notebooks/vcfs/G8.bcf',
##   's3://tiledb-inc-demo-data/examples/notebooks/vcfs/G9.bcf',
##   's3://tiledb-inc-demo-data/examples/notebooks/vcfs/G10.bcf']
```

You can add them to your existing dataset by re-opening it in _write_ mode and providing the file URIs. It's also necessary to allocate scratch space so the files can be downloaded to a temporary location prior to ingestion.

```python
small_ds = tiledbvcf.Dataset('small_dataset', mode = "w")
small_ds.ingest_samples(sample_uris)
```

The TileDB-VCF dataset located at `small_dataset` now includes records for 660 variants across 10 samples. The next section provides examples demonstrating how to query this dataset.
