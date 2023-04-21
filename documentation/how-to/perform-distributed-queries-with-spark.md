# Perform Distributed Queries with Spark

## Reading

TileDB-VCF's Spark API offers a `DataSourceV2` data source to read TileDB-VCF datasets into a Spark dataframe. To begin, launch a Spark shell with:

```bash
spark-shell \
    --jars build/libs/TileDB-VCF-Spark-0.1.0-SNAPSHOT.jar
```

Depending on the size of the dataset and query results, you may need to increase the configured memory from the defaults.:

```bash
spark-shell \
    --jars build/libs/TileDB-VCF-Spark-0.1.0-SNAPSHOT.jar \
    --driver-memory 16g \
    --executor-memory 16g
```

While the Spark API offers much of the same functionality provided by the CLI, the main considerations when using the Spark API are typically dataframe partitioning and memory overhead.

## Partitioning

There are two ways to partition a TileDB-VCF dataframe, which can be used separately or together:

* Partitioning over samples (`.option("sample_partitions", N)`).
* Partitioning over genomic regions (`.option("range_partitions", M)`).

Conceptually, these correspond to partitioning over rows and columns, respectively, in the underlying TileDB array. For example, if you are reading a subset of 200 samples in a dataset, and specify 10 sample partitions, Spark will create 10 jobs, each of which will be responsible for handling the export from 20 of the 200 selected samples.

Similarly, if the provided BED file contains 1,000,000 regions, and you specify 10 region partitions, Spark will create 10 jobs, each of which will be responsible for handling the export of 100,000 of the 1,000,000 regions (across all samples).

These two partitioning parameters can be composed to form rectangular regions of work to distribute across the available Spark executors.

::: {.callout-info}
The CLI interface offers the same partitioning feature, using the `--sample-partition` and `--region-partition` flags.&#x20;
:::

## Memory

Because TileDB-VCF is implemented as a native library, the Spark API makes use of both JVM on-heap and off-heap memory. This can make it challenging to correctly tune the memory consumption of each Spark job.

The `.option("memory", mb)` option is used as a best-effort memory budget that any particular job is allowed to consume, across _on-heap_ and _off-heap_ allocations. Increasing the available memory with this option can result in large performance improvements, provided the executors are configured with sufficient memory to avoid OOM failures.

## Examples

To export a few genomic regions from a dataset (which must be accessible by the Spark jobs; here we assume it is located on S3):

```scala
val df = spark.read.format("io.tiledb.vcf")\
        .option("uri", "s3://my-bucket/my-dataset")\
        .option("ranges", "chr1:1000-2000,chr2:500-501")\
        .option("samples", "sampleA,sampleB,sampleD")\
        .option("range_partitions", 2).load()

df.createOrReplaceTempView("vcf")

spark.sql("select contig, posStart, alleles, genotype from vcf").show
```

Because there are only two regions specified, and two region partitions, each of the two Spark jobs started will read one of the given regions.

We can also place a list of sample names and a list of genomic regions to read in explicit text files, and use those during reading:

```scala
val df = spark.read.format("io.tiledb.vcf")\
        .option("uri", "s3://my-bucket/my-dataset")\
        .option("bedfile", "s3://my-bucket/query.bed")\
        .option("samplefile", "s3://my-bucket/sample-names.txt")\
        .option("memory", 8*1024)\
        .option("sample_partitions", 10)\
        .option("range_partitions", 2).load()

df.createOrReplaceTempView("vcf")

spark.sql("select contig, posStart, alleles, genotype from vcf").show
```

Here, we've also increased the memory budget to 8GB, and added partitioning over samples.
