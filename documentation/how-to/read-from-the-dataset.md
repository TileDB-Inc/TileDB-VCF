# Read from the Dataset

## Basic Utils

Before slicing the data, you may wish to get some information about your dataset, such as the sample names, the attributes you can query, etc.

&#x20;You can get the **sample names** as follows:

```python
import tiledbvcf

uri = "my_vcf_dataset" 
ds = tiledbvcf.Dataset(uri, mode = "r") # open in "Read" mode
ds.samples()
```

```
tiledbvcf list -u my_vcf_dataset
```

You can get the **attributes** as follows:

```python
import tiledbvcf

uri = "my_vcf_dataset" 
ds = tiledbvcf.Dataset(uri, mode = "r") # open in "Read" mode
ds.attributes()                      # will print all queryable attributes
ds.attributes(attr_type = "builtin") # will print all materialized attributes
```

```
tiledbvcf stat -u my_vcf_datset
```

## Reading

You can _rapidly_ read from a TileDB-VCF dataset by providing three main parameters (all optional):

1. A subset of the samples
2. A subset of the attributes
3. One or more genomic ranges
   1. Either as strings in format `chr:pos_range`
   2. Or via a BED file

```python
import tiledbvcf

uri = "my_vcf_dataset" 
ds = tiledbvcf.Dataset(uri, mode = "r") # open in "Read" mode
ds.read(
    attrs = ["alleles", "pos_start", "pos_end"],
    regions = ["1:113409605-113475691", "1:113500000-113600000"],
    # or pass regions as follows:
    # bed_file = <bed_filename>
    samples = ['HG0099', 'HG00100']
)
```

```
tiledbvcf export \
    --uri my_vcf_dataset \
    --output-format t \
    --tsv-fields ALT,Q:POS,Q:END
    --sample-names HG0099,HG00100
    --regions 1:113409605-113475691,1:113500000-113600000
    # or pass the regions in a BED file as follows:
    # --regions-file <bed_filename>
```