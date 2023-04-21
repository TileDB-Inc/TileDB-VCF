# Python

## `tiledbvcf.dataset`

This is the main Python module.

### `Dataset`

Representation of the grouped TileDB arrays that constitute a TileDB-VCF dataset, which includes a sparse 3D array containing the actual variant data and a sparse 1D array containing various sample metadata and the VCF header lines. Read more about the data model [here](../data-model.md).

```python
Dataset(self, uri, mode='r', cfg=None, stats=False, verbose=False)
```

#### **Arguments**

* `uri`: URI of TileDB-VCF dataset
* `mode`: (default `'r'`) Open the array object in read `'r'` or write `'w'` mode
* `cfg`: TileDB-VCF configuration (optional)
* `stats`: (`bool`) Enable of disable TileDB stats
* `verbose`: (`bool`) Enable of disable TileDB-VCF verbose output

## `create_dataset()`

Create a new TileDB-VCF dataset.

```python
create_dataset(extra_attrs=None, tile_capacity=None, anchor_gap=None, checksum_type=None, allow_duplicates=True)
```

#### Arguments

* `extra_attrs`: (_list_ of _str_ attrs) list of extra attributes to materialize from the `FMT` or `INFO` field. Names should be `fmt_X` or `info_X` for a field name `X` (case sensitive).
* `tile_capacity`: (_int_) Tile capacity to use for the array schema (default `10000`)
* `anchor_gap`: (_int_) Length of gaps between inserted anchor records in bases (default = `1000`)
* `checksum_type`: (_str_ checksum) Optional override checksum type for creating new dataset (valid values are `'sha256'`, `'md5'` or `None`)
* `allow_duplicates`: (`bool`) Controls whether records with duplicate end positions can be ingested written to the dataset

### `ingest_samples()`

Ingest samples into an existing TileDB-VCF dataset.

```python
ingest_samples(sample_uris=None, threads=None, thread_task_size=None, memory_budget_mb=None, scratch_space_path=None, scratch_space_size=None, record_limit=None, sample_batch_size=None)
```

#### **Arguments:**

* `sample_uris`: (_list_ of _str_ samples) CSV list of VCF/BCF sample URIs to ingest
* `threads`: (_int_) Set the number of threads used for ingestion
* `thread_task_size`: (_int_) Set the max length (# columns) of an ingestion task (affects load balancing of ingestion work across threads and total memory consumption)
* `memory_budget_mb`: (_int_) Set the max size (MB) of TileDB buffers before flushing (default `1024`)
* record\_limit
* `str scratch_space_path`: (_`str`_) Directory used for local storage of downloaded remote samples
* `scratch_space_size`: (`int`) Amount of local storage that can be used for downloading remote samples (MB)
* `sample_batch_size`: (`int`) Number of samples per batch for ingestion (default `10`)
* `record_limit`: Limit the number of VCF records read into memory per file (default `50000`)
* `resume`: (`bool`) Whether to check and attempt to resume a partial completed ingestion

### `read()` / `read_arrow()`

Reads data from a TileDB-VCF dataset into a Pandas Dataframe (with `read()`) or a PyArrow Array (with `read_arrow()`).

```python
read(attrs, samples=None, regions=None, samples_file=None, bed_file=None, skip_check_samples=False)
```

#### **Arguments**

* `attrs`: (_list_ of _str_ attrs) List of attributes to extract. Can include attributes from the VCF _INFO_ and _FORMAT_ fields (prefixed with `info_` and `fmt_`, respectively) as well as any of the builtin attributes:
  * `sample_name`
  * `id`
  * `contig`
  * `alleles`
  * `filters`
  * `pos_start`
  * `pos_end`
  * `qual`
  * `query_bed_end`
  * `query_bed_start`
  * `query_bed_line`
* `samples`: (_list_ of _str_ samples) CSV list of sample names to be read
* `regions`: (_list_ of _str_ regions) CSV list of genomic regions to be read
* `samples_file`: (_str_  filesystem location) URI of file containing sample names to be read, one per line
* `bed_file`: (_str_ filesystem location) URI of a BED file of genomic regions to be read
* `skip_check_samples`: (_bool_) Should checking the samples requested exist in the array
* `disable_progress_estimation`: (_bool_) Should we skip estimating the progress in verbose mode? Estimating progress can have performance or memory impacts in some cases.

#### **Details**

For large datasets, a call to `read()` may not be able to fit all results in memory. In that case, the returned dataframe will contain as many results as possible, and in order to retrieve the rest of the results, use the `continue_read()` function.

You can also use the Python generator version, `read_iter()`.

**Returns**: Pandas `DataFrame` containing results.

### `read_completed()`

```python
read_completed()
```

#### **Details**

A read is considered complete if the resulting dataframe contained all results.

**Returns:** (`bool`) `True` if the previous read operation was complete

### `count()`

Counts data in a TileDB-VCF dataset.

```python
count(samples=None, regions=None)
```

#### **Arguments**

* `samples`: (_list_ of _str_ samples) CSV list of sample names to include in the count
* `regions`: (_list_ of _str_ regions) CSV list of genomic regions to include in the count

#### **Details**

**Returns:** Number of intersecting records in the dataset

### `attributes()`

List queryable attributes available in the VCF dataset

```python
attributes(attr_type = "all")
```

#### **Arguments**

* `attr_type`: (_list_ of _str_ attributes) The subset of attributes to retrieve; `"info"` or `"fmt"` will only retrieve attributes ingested from the VCF `INFO` and `FORMAT` fields, respectively, `"builtin"` retrieves the static attributes defined in TileDB-VCF's schema, `"all"` (the default) returns all queryable attributes

#### **Details**

**Returns:** a list of strings representing the attribute names

## `tiledbvcf.ReadConfig`

Set various configuration parameters.

```python
ReadConfig(limit, region_partition, sample_partition, sort_regions, memory_budget_mb, tiledb_config)
```

**Parameters**

* `limit`: max number of records (rows) to read
* `region_partition`: Region partition tuple (idx, num\_partitions)
* `sample_partition`: Samples partition tuple (idx, num\_partitions)
* `sort_regions`: Whether or not to sort the regions to be read (default `True`)
* `memory_budget_mb`: Memory budget (MB) for buffer and internal allocations (default `2048`)
* `buffer_percentage`: Percentage of memory to dedicate to TileDB Query Buffers (default: `25`)
* `tiledb_tile_cache_percentage`: Percentage of memory to dedicate to TileDB Tile Cache (default: `10`)
* `tiledb_config`: List of strings in the format `"option=value"` (see [here](broken-reference) for full list TileDB configuration parameters)

## `tiledbvcf.dask`

This module is for the TileDB-VCF integration with Dask.

### `read_dask()`

Reads data from a TileDB-VCF dataset into a Dask `DataFrame`.

```python
read_dask(attrs, region_partitions=1, sample_partitions=1, samples=None, regions=None, samples_file=None, bed_file=None)
```

#### Arguments

* `attrs`: (_list_ of _str_ attrs) List of attribute names to be read
* `region_partitions` (_int_ partitions) Number of partitions over regions
* `sample_partitions` (_int_ partitions) Number of partitions over samples
* `samples`: (_list_ of _str_ samples) CSV list of sample names to be read
* `regions`: (_list_ of _str_ regions) CSV list of genomic regions to be read
* `samples_file`: (_str_  filesystem location) URI of file containing sample names to be read, one per line
* `bed_file`: (_str_ filesystem location) URI of a BED file of genomic regions to be read

#### Details

Partitioning proceeds by a straightforward block distribution, parameterized by the total number of partitions and the particular partition index that a particular read operation is responsible for.

Both region and sample partitioning can be used together.

**Returns**: Dask `DataFrame` with results

### `map_dask()`

Maps a function on a Dask `DataFrame` obtained by reading from the dataset.

```python
map_dask(fnc, attrs, region_partitions=1, sample_partitions=1, samples=None, regions=None, samples_file=None, bed_file=None)
```

#### Arguments

* `fnc`: (_function)_ Function applied to each partition
* `attrs`: (_list_ of _str_ attrs) List of attribute names to be read
* `region_partitions` (_int_ partitions) Number of partitions over regions
* `sample_partitions` (_int_ partitions) Number of partitions over samples
* `samples`: (_list_ of _str_ samples) CSV list of sample names to be read
* `regions`: (_list_ of _str_ regions) CSV list of genomic regions to be read
* `samples_file`: (_str_  filesystem location) URI of file containing sample names to be read, one per line
* `bed_file`: (_str_ filesystem location) URI of a BED file of genomic regions to be read

#### Details

May be more efficient in some cases than `read_dask()` followed by a regular Dask map operation.

**Returns**: Dask `DataFrame` with results
