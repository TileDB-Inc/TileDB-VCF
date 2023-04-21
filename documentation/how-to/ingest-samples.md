# Ingest Samples

::: {.callout-info}
Indexed files are required for ingestion. If your VCF/BCF files have not been indexed, you can use [`bcftools`](https://samtools.github.io/bcftools/bcftools.html)to do so:
:::

```bash
for f in data/vcfs/*.vcf.gz; do bcftools index -c $f; done
```

You can ingest samples into an already created dataset as follows:

```python
import tiledbvcf

uri = "my_vcf_dataset" 
ds = tiledbvcf.Dataset(uri, mode = "w")
ds.ingest_samples(sample_uris = ["sample_1", "samples_2"])
```

Just add a regular expression for the VCF file locations at the end of the `store` command:

```bash
tiledbvcf store --uri my_vcf_dataset *.bcf 
```

Alternatively, provide a text file with the absolute locations of the VCF files, separated by a new line:

```
tiledbvcf store --uri my_vcf_dataset --samples-file samples.txt
```

::: {.callout-info}
**Incremental updates** work in the same manner as the ingestion above, nothing special is needed. In addition, the ingestion is thread- and process-safe and, therefore, can be performed _in parallel_.
:::