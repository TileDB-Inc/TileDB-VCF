# Perform Distributed Queries with TileDB-Cloud

The `tiledbvcf` Python package includes integration with [TileDB-Cloud](https://cloud.tiledb.com) to enable distributing large queries in a serverless maner.&#x20;

## Task Graphs

You can use the `tiledbvcf` package's TileDB Cloud integration to partition read operations across regions and samples. The partitioning semantics are identical to those used by the CLI and Spark.&#x20;

```python
import tiledbvcf
import tiledb.cloud.vcf

tiledb.cloud.vcf.query.read('my-large-dataset',
                       attrs=['sample_name', 'pos_start', 'pos_end'],
                       bed_file='very-large-bedfile.bed',
                       region_partitions=10,
                       sample_partitions=2)
```

The result is a pyarrow table.