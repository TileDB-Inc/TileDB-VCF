# Perform Distributed Queries with Dask

The `tiledbvcf` Python package includes integration with [Dask](https://dask.org) to enable distributing large queries across node clusters.&#x20;

## Dask DataFrames

You can use the `tiledbvcf` package's Dask integration to partition read operations across regions and samples. The partitioning semantics are identical to those used by the CLI and Spark.&#x20;

```python
import tiledbvcf
import dask
dask.config.set({'dataframe.query-planning': False})

ds = tiledbvcf.Dataset('my-large-dataset', mode='r')
dask_df = ds.read_dask(attrs=['sample_name', 'pos_start', 'pos_end'],
                       bed_file='very-large-bedfile.bed',
                       region_partitions=10,
                       sample_partitions=2)
```

The result is a Dask dataframe (rather than a Pandas dataframe). We're using a local machine for simplicity but the API works on any Dask cluster.&#x20;

## Map Operations

If you plan to perform filter the results in a Dask dataframe, it may be more efficient to use `map_dask()` rather than `read_dask()`. The `map_dask()` function takes an additional parameter, `fnc`, allowing you to provide a filtering function that is applied immediately after performing the read but before inserting the result of the partition into the Dask dataframe.

In the following example, any variants overlapping regions in `very-large-bedfile.bed` are filtered out if their start position overlaps the first 25kb of the chromosome.

```python
import tiledbvcf

ds = tiledbvcf.TileDBVCFDataset('my-large-dataset', mode='r')
dask_df = ds.map_dask(lambda df: df[df.pos_start < 25000],
                      attrs=['sample_name', 'pos_start', 'pos_end'],
                      bed_file='very-large-bedfile.bed',
                      region_partitions=10,
                      sample_partitions=2)
```

This approach can be more efficient than using `read_dask()` with a separate filtering step because it avoids the possibility that partitions require multiple read operations due to memory constraints.&#x20;

The pseudocode describing the `read_partition()` algorithm (i.e., the code responsible for reading the partition on a Dask worker) is:

```python
ds = tiledbvcf.Dataset(uri, mode='r')
overall_df = ds.read(attrs, samples, regions, ...)
while not ds.read_completed():
    df = ds.continue_read()
    overall_df = overall_df.append(df)
```

When using `map_dask()` instead, the pseudocode becomes:

```python
ds = tiledbvcf.Dataset(uri, mode='r')
overall_df = filter_fnc(ds.read(attrs, samples, regions, ...))
while not ds.read_completed():
    df = filter_fnc(ds.continue_read())
    overall_df = overall_df.append(df)
```

You can see that if the provided `filter_fnc()` reduces the size of the data substantially, using `map_dask()` can reduce the likelihood that the Dask workers will run out of memory and avoid needing to perform multiple reads.
