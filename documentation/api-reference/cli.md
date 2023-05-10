# CLI

```
TileDB-VCF -- Efficient variant-call data storage and retrieval.

  This command-line utility provides an interface to create, store and
  efficiently retrieve variant-call data in the TileDB storage format.

  More information: TileDB-VCF <https://tiledb-inc.github.io/TileDB-VCF>

Usage: tiledbvcf [OPTIONS] SUBCOMMAND

Options:
  -h,--help                             Print this help message and exit
  -v,--version                          Print the version information and exit

Subcommands:
  create                                Creates an empty TileDB-VCF dataset
  store                                 Ingests samples into a TileDB-VCF dataset
  delete                                Delete samples from a TileDB-VCF dataset
  export                                Exports data from a TileDB-VCF dataset
  list                                  Lists all sample names present in a TileDB-VCF dataset
  stat                                  Prints high-level statistics about a TileDB-VCF dataset
  utils                                 Utils for working with a TileDB-VCF dataset
  version                               Print the version information and exit
```

## Create

```
Creates an empty TileDB-VCF dataset

Usage: tiledbvcf create [OPTIONS]

Options:
  -u,--uri TEXT REQUIRED                TileDB-VCF dataset URI
  -a,--attributes TEXT=[] ... Excludes: --vcf-attributes
                                        INFO and/or FORMAT field names (comma-delimited) to store as separate attributes.
                                        Names should be 'fmt_X' or 'info_X' for a field name 'X' (case sensitive).
  -v,--vcf-attributes TEXT Excludes: --attributes
                                        Create separate attributes for all INFO and FORMAT fields in the provided VCF file.
  -g,--anchor-gap UINT=1000             Anchor gap size to use
  -n,--no-duplicates                    Allow records with duplicate start positions to be written to the array.
  --compress-sample-dim,--no-compress-sample-dim{false}
                                        Enable/disable compression of the sample dimension. Enabled by default.

Ingestion task options:
  --enable-allele-count,--disable-allele-count{false}
                                        Enable/disable allele count array creation. Enabled by default.
  --enable-variant-stats,--disable-variant-stats{false}
                                        Enable/disable variant stats array creation. Enabled by default.

TileDB options:
  -c,--tile-capacity UINT=10000         Tile capacity to use for the array schema
  --tiledb-config TEXT=[] ...           CSV string of the format 'param1=val1,param2=val2...' specifying optional TileDB
                                        configuration parameter settings.
  --checksum ENUM:value in {md5->md5,none->none,sha256->sha256} OR {md5,none,sha256}=sha256
                                        Checksum to use for dataset validation on read and writes.

Debug options:
  --log-level TEXT:{fatal,error,warn,info,debug,trace}=fatal
                                        Log message level
  --log-file TEXT                       Log message output file
```

## Store

```
Ingests samples into a TileDB-VCF dataset

Usage: tiledbvcf store [OPTIONS] [paths...]

Positionals:
  paths TEXT=[] ... Excludes: --samples-file
                                        VCF URIs to ingest

Options:
  -u,--uri TEXT REQUIRED                TileDB-VCF dataset URI
  -t,--threads UINT=20                  Number of threads
  -m,--total-memory-budget-mb UINT:UINT in [512 - 64103]=48077
                                        The total memory budget for ingestion (MiB)
  -M,--total-memory-percentage FLOAT:FLOAT in [0 - 1]=0
                                        Percentage of total system memory used for ingestion (overrides '--total-memory-budget-mb')
  --resume                              Resume incomplete ingestion of sample batch

Sample options:
  -e,--sample-batch-size UINT=10        Number of samples per batch for ingestion
  -f,--samples-file TEXT Excludes: paths
                                        File with 1 VCF path to be ingested per line. The format can also include an explicit
                                        index path on each line, in the format '<vcf-uri><TAB><index-uri>'
  --remove-sample-file Needs: --samples-file
                                        If specified, the samples file ('-f' argument) is deleted after successful ingestion
  -d,--scratch-dir TEXT                 Directory used for local storage of downloaded remote samples
  -s,--scratch-mb UINT=0                Amount of local storage that can be used for downloading remote samples (MB)

TileDB options:
  -p,--s3-part-size UINT=50             [S3 only] Part size to use for writes (MB)
  --tiledb-config TEXT=[] ...           CSV string of the format 'param1=val1,param2=val2...' specifying optional TileDB
                                        configuration parameter settings.
  --stats                               Enable TileDB stats
  --stats-vcf-header-array              Enable TileDB stats for vcf header array usage

Advanced options:
  --ratio-tiledb-memory FLOAT:FLOAT in [0.01 - 0.99]=0.5
                                        Ratio of memory budget allocated to TileDB::sm.mem.total_budget
  --max-tiledb-memory-mb UINT=4096      Maximum memory allocated to TileDB::sm.mem.total_budget (MiB)
  --input-record-buffer-mb UINT=1       Size of input record buffer for each sample file (MiB)
  --avg-vcf-record-size INT:INT in [1 - 4096]=512
                                        Average VCF record size (bytes)
  --ratio-task-size FLOAT:FLOAT in [0.01 - 1]=0.75
                                        Ratio of worker task size to computed task size
  --ratio-output-flush FLOAT:FLOAT in [0.01 - 1]=0.75
                                        Ratio of output buffer capacity that triggers a flush to TileDB

Contig options:
  --disable-contig-fragment-merging{false} Excludes: --contigs-to-keep-separate --contigs-to-allow-merging
                                        Disable merging of contigs into fragments. Generally contig fragment merging is good,
                                        this is a performance optimization to reduce the prefixes on a s3/azure/gcs bucket
                                        when there is a large number of pseudo contigs which are small in size.
  --contigs-to-keep-separate TEXT ... Excludes: --disable-contig-fragment-merging --contigs-to-allow-merging
                                        Comma-separated list of contigs that should not be merged into combined fragments.
                                        The default list includes all standard human chromosomes in both UCSC (e.g., chr1)
                                        and Ensembl (e.g., 1) formats.
  --contigs-to-allow-merging TEXT=[] ... Excludes: --disable-contig-fragment-merging --contigs-to-keep-separate
                                        Comma-separated list of contigs that should be allowed to be merged into combined
                                        fragments.
  --contig-mode ENUM:value in {all->all,merged->merged,separate->separate} OR {all,merged,separate}=all
                                        Select which contigs are ingested: 'separate', 'merged', or 'all' contigs

Debug options:
  --log-level TEXT:{fatal,error,warn,info,debug,trace}=fatal
                                        Log message level
  --log-file TEXT                       Log message output file
  -v,--verbose :DEPRECATED              Enable verbose output DEPRECATED: please use '--log-level debug' instead

Legacy options:
  -n,--max-record-buff UINT             Max number of VCF records to buffer per file
  -k,--thread-task-size UINT            Max length (# columns) of an ingestion task. Affects load balancing of ingestion
                                        work across threads, and total memory consumption.
  -b,--mem-budget-mb UINT               The maximum size of TileDB buffers before flushing (MiB)
```

## Delete

```
Delete samples from a TileDB-VCF dataset

Usage: tiledbvcf delete [OPTIONS]

Options:
  -u,--uri TEXT REQUIRED                TileDB-VCF dataset URI
  -s,--sample-names TEXT=[] ...         CSV list of sample names to delete
  --tiledb-config TEXT=[] ...           CSV string of the format 'param1=val1,param2=val2...' specifying optional TileDB
                                        configuration parameter settings.
  --log-level TEXT:{fatal,error,warn,info,debug,trace}=fatal
                                        Log message level
  --log-file TEXT                       Log message output file
```

## Export

```
Exports data from a TileDB-VCF dataset

Usage: tiledbvcf export [OPTIONS]

Options:
  -u,--uri TEXT REQUIRED                TileDB-VCF dataset URI

Output options:
  -O,--output-format ENUM:value in {b->b,t->t,u->u,v->v,z->z} OR {b,t,u,v,z}=b
                                        Export format. Options are: 'b': bcf (compressed); 'u': bcf; 'z': vcf.gz; 'v': vcf;
                                        't': TSV
  -o,--output-path TEXT                 [TSV or combined VCF export only] The name of the output file.
  -m,--merge Needs: --output-path       Export combined VCF file.
  -t,--tsv-fields TEXT=[] ...           [TSV export only] An ordered CSV list of fields to export in the TSV. A field name
                                        can be one of 'SAMPLE', 'ID', 'REF', 'ALT', 'QUAL', 'POS', 'CHR', 'FILTER'. Additionally,
                                        INFO fields can be specified by 'I:<name>' and FMT fields with 'F:<name>'. To export
                                        the intersecting query region for each row in the output, use the field names 'Q:POS',
                                        'Q:END' and 'Q:LINE'.
  -n,--limit UINT=18446744073709551615  Only export the first N intersecting records.
  -d,--output-dir TEXT                  Directory used for local output of exported samples
  --upload-dir TEXT                     If set, all output file(s) from the export process will be copied to the given directory
                                        (or S3 prefix) upon completion.
  -c,--count-only Excludes: --af-filter Don't write output files, only print the count of the resulting number of intersecting
                                        records.
  --af-filter TEXT Excludes: --count-only
                                        If set, only export data that passes the AF filter.

Region options:
  -r,--regions TEXT=[] ... Excludes: --regions-file
                                        CSV list of regions to export in the format 'chr:min-max'
  -R,--regions-file TEXT Excludes: --regions
                                        File containing regions (BED format)
  --sorted                              Do not sort regions or regions file if they are pre-sorted
  --region-partition TEXT               Partitions the list of regions to be exported and causes this export to export only
                                        a specific partition of them. Specify in the format I:N where I is the partition
                                        index and N is the total number of partitions. Useful for batch exports.

Sample options:
  -f,--samples-file TEXT Excludes: --sample-names
                                        Path to file with 1 sample name per line
  -s,--sample-names TEXT=[] ... Excludes: --samples-file
                                        CSV list of sample names to export
  --sample-partition TEXT               Partitions the list of samples to be exported and causes this export to export only
                                        a specific partition of them. Specify in the format I:N where I is the partition
                                        index and N is the total number of partitions. Useful for batch exports.
  --disable-check-samples{false}        Disable validating that sample passed exist in dataset before executing query and
                                        error if any sample requested is not in the dataset

TileDB options:
  --tiledb-config TEXT=[] ...           CSV string of the format 'param1=val1,param2=val2...' specifying optional TileDB
                                        configuration parameter settings.
  --mem-budget-buffer-percentage FLOAT=25
                                        The percentage of the memory budget to use for TileDB query buffers.
  --mem-budget-tile-cache-percentage FLOAT=10
                                        The percentage of the memory budget to use for TileDB tile cache.
  -b,--mem-budget-mb UINT=2048          The memory budget (MB) used when submitting TileDB queries.
  --stats                               Enable TileDB stats
  --stats-vcf-header-array              Enable TileDB stats for vcf header array usage

Debug options:
  --log-level TEXT:{fatal,error,warn,info,debug,trace}=fatal
                                        Log message level
  --log-file TEXT                       Log message output file
  -v,--verbose :DEPRECATED              Enable verbose output DEPRECATED: please use '--log-level debug' instead
  --enable-progress-estimation          Enable progress estimation in verbose mode. Progress estimation can sometimes cause
                                        a performance impact, so enable this with consideration.
  --debug-print-vcf-regions             Enable debug printing of vcf region passed by user or bed file. Requires verbose
                                        mode
  --debug-print-sample-list             Enable debug printing of sample list used in read. Requires verbose mode
  --debug-print-tiledb-query-ranges     Enable debug printing of tiledb query ranges used in read. Requires verbose mode
```

## List

```
Lists all sample names present in a TileDB-VCF dataset

Usage: tiledbvcf list [OPTIONS]

Options:
  -u,--uri TEXT REQUIRED                TileDB-VCF dataset URI
  --tiledb-config TEXT=[] ...           CSV string of the format 'param1=val1,param2=val2...' specifying optional TileDB
                                        configuration parameter settings.
  --log-level TEXT:{fatal,error,warn,info,debug,trace}=fatal
                                        Log message level
  --log-file TEXT                       Log message output file
```

## Stat

```
Prints high-level statistics about a TileDB-VCF dataset

Usage: tiledbvcf stat [OPTIONS]

Options:
  -u,--uri TEXT REQUIRED                TileDB-VCF dataset URI
  --tiledb-config TEXT=[] ...           CSV string of the format 'param1=val1,param2=val2...' specifying optional TileDB
                                        configuration parameter settings.
  --log-level TEXT:{fatal,error,warn,info,debug,trace}=fatal
                                        Log message level
  --log-file TEXT                       Log message output file
```

## Consolidate

```
Consolidate TileDB-VCF dataset

Usage: tiledbvcf utils consolidate [OPTIONS] SUBCOMMAND

Options:
  -h,--help                             Print this help message and exit

Subcommands:
  commits                               Consolidate TileDB-VCF dataset commits
  fragments                             Consolidate TileDB-VCF dataset fragments
  fragment_meta                         Consolidate TileDB-VCF dataset fragment metadata
```

## Vacuum

```
Vacuum TileDB-VCF dataset

Usage: tiledbvcf utils vacuum [OPTIONS] SUBCOMMAND

Options:
  -h,--help                             Print this help message and exit

Subcommands:
  commits                               Vacuum TileDB-VCF dataset commits
  fragments                             Vacuum TileDB-VCF dataset fragments
  fragment_meta                         Vacuum TileDB-VCF dataset fragment metadata
```

