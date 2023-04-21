# Export to VCF

You can use the TileDB-VCF CLI to export the TileDB-VCF ingested dataset back into VCF formats for downstream analyses, in a **lossless** way.

::: {.callout-warning}
While these exports are lossless in terms of the actual data stored, they _may not be identical_ to the original files. For example, fields within the `INFO` and `FORMAT` columns may appear in a slightly different order in the exported files.
:::

## Basic export

To recreate all of original (single-sample) VCF files simply run the `export` command and set the `--output-format`to `v`, for VCF.&#x20;

```bash
tiledbvcf export \
  --uri my_vcf_dataset \
  --output-format v \
  --output-dir exported-vcfs
```

If `bcftools` is available on your system you can use it to easily examine any of the exported files:

```bash
bcftools view --no-header exported-vcfs/G1.vcf

## 1    13350    .    A    <NON_REF>    .    .    END=36258    GT:DP:GQ:MIN_DP:PL    1/0:50:3:43:44,29,99
## 1    42091    .    A    <NON_REF>    .    .    END=101445    GT:DP:GQ:MIN_DP:PL    0/0:8:91:60:35,62,92
## 2    11625    .    T    <NON_REF>    .    .    END=106375    GT:DP:GQ:MIN_DP:PL    0/0:27:72:76:70,30,83
## 3    14580    .    T    <NON_REF>    .    .    END=86190    GT:DP:GQ:MIN_DP:PL    0/1:50:78:41:67,11,43
```

## Filtering variants

The same mechanics covered in the [reading](read-from-the-dataset.md) for filtering records by sample and genomic region also apply to exporting VCF files. &#x20;

```bash
tiledbvcf export \
  --uri my_vcf_dataset \
  --output-format v \
  --sample-names G1,G2,G3 \
  --regions 4:53227-196092,9:214865-465259 \
  --output-dir exported-filtered-vcfs
```
