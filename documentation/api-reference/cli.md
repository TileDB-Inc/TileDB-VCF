# CLI

## Available Commands

1. `create`a new dataset
2. `store`specified VCF files in a dataset
3. `export`data from a dataset
4. `list`all sample names present in a dataset
5. `stat`prints high-level statistics about a dataset
6. `utils` utility functions for dataset

## Create

Create an empty TileDB-VCF dataset.

### Usage

```bash
tiledbvcf create -u <uri> [-a <fields>] [-c <N>] [-g <N>] [--tiledb-config <params>] [--checksum <checksum>] [-n]
```

### Options

| Flag                   | Description                                                                                                                                              |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-u`,`--uri`           | TileDB dataset URI.                                                                                                                                      |
| `-a`,`--attributes`    | Info or format field names (comma-delimited) to store as separate attributes. Names should be `fmt_X` or `info_X` for a field name `X` (case sensitive). |
| `-c`,`--tile-capacity` | Tile capacity to use for the array schema \[default `10000`].                                                                                            |
| `-g`,`--anchor-gap`    | Anchor gap size to use \[default `1000`].                                                                                                                |
| `--tiledb-config`      | CSV string of the format `'param1=val1,param2=val2...'` specifying optional TileDB configuration parameter settings.                                     |
| `--checksum`           | Checksum to use for dataset validation on read and writes \[default `"sha256"`].                                                                         |
| `-n`,`--no-duplicates` | Do not allow records with duplicate end positions to be written to the array.                                                                            |

## Store

Ingests registered samples into a TileDB-VCF dataset.

### Usage

```bash
tiledbvcf store -u <uri> [-t <N>] [-p <MB>] [-d <path>] [-s <MB>] [-n <N>] [-k <N>] [-b <MB>] [-v] [--remove-sample-file] [--tiledb-config <params>] ([-f <path>] | <paths>...) [-e <N>] [--stats] [--stats-vcf-header-array]
```

### Options

| Flag                        | Description                                                                                                                                                                                                                                                                                                                         |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-u`,`--uri`                | TileDB dataset URI.                                                                                                                                                                                                                                                                                                                 |
| `-t`,`--threads`            | Number of threads \[default `16`].                                                                                                                                                                                                                                                                                                  |
| `-p`,`--s3-part-size`       | \[S3 only] Part size to use for writes (MB) \[default `50`].                                                                                                                                                                                                                                                                        |
| `-d`,`--scratch-dir`        | Directory used for local storage of downloaded remote samples.                                                                                                                                                                                                                                                                      |
| `-s`,`--scratch-mb`         | Amount of local storage (in MB) allocated for downloading remote VCF files prior to ingestion  \[default `0`]. The you must configure enough scratch space to hold at least 20 samples. In general, you need 2 × the sample dimension's sample\_bactch\_size (which by default is 10). You can read more about the data model here. |
| `-n`, `--max-record-buff`   | Max number of BCF records to buffer per file \[default `50000`].                                                                                                                                                                                                                                                                    |
| `-k`, `--thread-task-size`  | Max length (# columns) of an ingestion task. Affects load balancing of ingestion work across threads, and total memory consumption \[default `5000000`].                                                                                                                                                                            |
| `-b`, `--mem-budget-mb`     | The total memory budget (MB) used when submitting TileDB queries \[default `1024`].                                                                                                                                                                                                                                                 |
| `-v`, `--verbose`           | Enable verbose output.                                                                                                                                                                                                                                                                                                              |
| `--tiledb-config`           | CSV string of the format `'param1=val1,param2=val2...'` specifying optional TileDB configuration parameter settings.                                                                                                                                                                                                                |
| `-f`, `--samples-file`      | File with 1 VCF path to be ingested per line. The format can also include an explicit index path on each line, in the format `<vcf-uri><TAB><index-uri>`.                                                                                                                                                                           |
| `--remove-sample-file`      | If specified, the samples file (`-f` argument) is deleted after successful ingestion                                                                                                                                                                                                                                                |
| `-e`, `--sample-batch-size` | Number of samples per batch for ingestion \[default `10`].                                                                                                                                                                                                                                                                          |
| `--stats`                   | Enable TileDB stats                                                                                                                                                                                                                                                                                                                 |
| `--stats-vcf-header-array`  | Enable TileDB stats for vcf header array usage.                                                                                                                                                                                                                                                                                     |
| `--resume`                  | Resume incomplete ingestion of sample batch.                                                                                                                                                                                                                                                                                        |

## Export

Exports data from a TileDB-VCF dataset.

### Usage

```bash
tiledbvcf export -u <uri> [-O <format>] [-o <path>] [-t <fields>] ([-r <regions>] | [-R <path>]) [--sorted] [-n <N>] [-d <path>] [--sample-partition <I:N>] [--region-partition <I:N>] [--upload-dir <path>] [--tiledb-config <params>] [-v] [-c] [-b <MB>] ([-f <path>] | [-s <samples>]) [--stats] [--stats-vcf-header-array]
```

### Options

| Flag                                 | Description                                                                                                                                                                                                                                                                                                                                                            |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-u`,`--uri`                         | TileDB dataset URI.                                                                                                                                                                                                                                                                                                                                                    |
| `-O`,`--output-format`               | Export format. Options are: `b`: bcf (compressed); `u`: bcf; `z`: vcf.gz; `v`: vcf; `t`: TSV. \[default `b`] .                                                                                                                                                                                                                                                         |
| `-o`,`--output-path`                 | \[TSV export only] The name of the output TSV file.                                                                                                                                                                                                                                                                                                                    |
| `-t`,`--tsv-fields`                  | \[TSV export only] An ordered CSV list of fields to export in the TSV. A field name can be one of `SAMPLE`, `ID`, `REF`, `ALT`, `QUAL`, `POS`, `CHR`, `FILTER`. Additionally, `INFO` fields can be specified by `I` and `FMT` fields with `S`. To export the intersecting query region for each row in the output, use the field names `Q:POS`, `Q:END`, or `Q:LINE`.  |
| `-r`,`--regions`                     | CSV list of regions to export in the format `chr:min-max`.                                                                                                                                                                                                                                                                                                             |
| `-R`,`--regions-file`                | File containing regions (BED format).                                                                                                                                                                                                                                                                                                                                  |
| `--sorted`                           | Do not sort regions or regions file if they are pre-sorted.                                                                                                                                                                                                                                                                                                            |
| `-n`,`--limit`                       | Only export the first _N_ intersecting records.                                                                                                                                                                                                                                                                                                                        |
| `-d`,`--output-dir`                  | Directory used for local output of exported samples.                                                                                                                                                                                                                                                                                                                   |
| `--sample-partition`                 | Partitions the list of samples to be exported and causes this export to export only a specific partition of them. Specify in the format `I:N` where `I` is the partition index and `N` is the total number of partitions. Useful for batch exports.                                                                                                                    |
| `--region-partition`                 | Partitions the list of regions to be exported and causes this export to export only a specific partition of them. Specify in the format `I:N` where `I` is the partition index and `N` is the total number of partitions. Useful for batch exports.                                                                                                                    |
| `--upload-dir`                       | If set, all output file(s) from the export process will be copied to the given directory (or S3 prefix) upon completion.                                                                                                                                                                                                                                               |
| `--tiledb-config`                    | CSV string of the format `'param1=val1,param2=val2...'` specifying optional TileDB configuration parameter settings.                                                                                                                                                                                                                                                   |
| `-v`,`--verbose`                     | Enable verbose output.                                                                                                                                                                                                                                                                                                                                                 |
| `-c`,`--count-only`                  | Don't write output files, only print the count of the resulting number of intersecting records.                                                                                                                                                                                                                                                                        |
| `-b`,`--mem-budget-mb`               | The memory budget (MB) used when submitting TileDB queries \[default `2048`].                                                                                                                                                                                                                                                                                          |
| `--mem-budget-buffer-percentage`     | The percentage of the memory budget to use for TileDB query buffers  \[default `25`].                                                                                                                                                                                                                                                                                  |
| `--mem-budget-tile-cache-percentage` | The percentage of the memory budget to use for TileDB tile cache \[default `10`].                                                                                                                                                                                                                                                                                      |
| `-f`,`--samples-file`                | File with 1 VCF path to be registered per line. The format can also include an explicit index path on each line, in the format in the format `<vcf-uri><TAB><index-uri>`.                                                                                                                                                                                              |
| `--stats`                            | Enable TileDB stats                                                                                                                                                                                                                                                                                                                                                    |
| `--stats-vcf-header-array`           | Enable TileDB stats for vcf header array usage.                                                                                                                                                                                                                                                                                                                        |
| `--disable-check-samples`            | Disable validating that samples passed exist in dataset before executing query and error if any sample requested is not in the dataset.                                                                                                                                                                                                                                |
| `--disable-progress-estimation`      | Disable progress estimation in verbose mode. Progress estimation can sometimes cause a performance impact.                                                                                                                                                                                                                                                             |

## List

Lists all sample names present in a TileDB-VCF dataset.

### Usage

```bash
tiledbvcf list -u <uri> [--tiledb-config ]
```

### Options

| Flag              | Description                                                                                                          |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| `-u`,`--uri`      | TileDB dataset URI.                                                                                                  |
| `--tiledb-config` | CSV string of the format `'param1=val1,param2=val2...'` specifying optional TileDB configuration parameter settings. |

## Stat

Prints high-level statistics about a TileDB-VCF dataset.

### Usage

```bash
tiledbvcf stat -u <uri> [--tiledb-config ]
```

### Options

| Flag              | Description                                                                                                          |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| `-u`,`--uri`      | TileDB dataset URI.                                                                                                  |
| `--tiledb-config` | CSV string of the format `'param1=val1,param2=val2...'` specifying optional TileDB configuration parameter settings. |

## Utils

Utils for working with a TileDB-VCF dataset, such for consolidating and vacuuming fragments or fragment metadata.

### Usage

```bash
tiledbvcf utils (consolidate|vacuum) (fragment_meta|fragments) -u <uri> [--tiledb-config ]
```

### Options

| Flag              | Description                                                                                                          |
| ----------------- | -------------------------------------------------------------------------------------------------------------------- |
| `-u`,`--uri`      | TileDB dataset URI.                                                                                                  |
| `--tiledb-config` | CSV string of the format `'param1=val1,param2=val2...'` specifying optional TileDB configuration parameter settings. |
