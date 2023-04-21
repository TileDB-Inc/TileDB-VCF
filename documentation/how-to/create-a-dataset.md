# Create a Dataset

The first step before ingesting any VCF samples is to create a dataset. This effectively creates a TileDB group and the appropriate empty arrays in it.&#x20;

```python
import tiledbvcf

uri = "my_vcf_dataset" 
ds = tiledbvcf.Dataset(uri, mode = "w") # sets dataset to "Write" mode
ds.create_dataset()                     # creates the dataset and
                                        # keeps it in "Write" mode
```

```bash
tiledbvcf create --uri my_vcf_dataset

If you wish to turn some of the `INFO` and `FMT` fields into separate _materialized_ attributes, you can do so as follows (names should be `fmt_X` or `info_X` for a field name `X` - case sensitive).

```python
import tiledbvcf

uri = "my_vcf_dataset" 
ds = tiledbvcf.Dataset(uri, mode = "w") 
ds.create_dataset(extra_attrs=["info_AA"])
```

```bash
tiledbvcf create --uri my_vcf_dataset --attributes info_AA
```