---
title: Distributed Ingestion
---

TileDB Cloud has built in support for simple distributed ingestion.

`ingest` is a simply python command that will dispatch and run a [task graph](https://docs.tiledb.com/cloud/concepts/tiledb-cloud-internals/task-graphs) to load VCF samples in parallel across a number of machines.

## Example

In order to run this example please make sure to have install TileDB-Cloud-Py with `pip install --user tiledb-cloud`.

```python
import tiledb.cloud
from tiledb.cloud.vcf import ingest

s3_storage_uri = "s3://my_bucket/my_array"
vcf_location = "s3://1000genomes-dragen-v3.7.6/data/individuals/hg38-graph-based"
pattern = "*.hard-filtered.vcf.gz"
max_files = 75
name = f"dragen-v3.7.6-example-{max_files}"

namespace = "my-organization"
tiledb_uri = f"tiledb://{namespace}/{name}"

# Define which contigs we want to ingest
contigs = Contigs.CHROMOSOMES


ingest(
    s3_storage_uri,
    config=config,
    search_uri=vcf_location,
    pattern=pattern,
    max_files=max_files,
    contigs=contigs,
)
```

### Contigs

TileDB support specifying the contigs you wish to ingest. The default behavior is to ingestion all contigs present in a VCF file. However you can specify if you'd like to restrict to a specific list or a predefined list

Options for contigs:

- `ALL`
- `CHROMOSOMES`
- `OTHER`