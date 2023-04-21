# Handle Large Queries

Unlike TileDB-VCF's CLI, which exports directly to disk, results for queries performed using Python are read into memory. Therefore, when querying even moderately sized genomic datasets, the amount of available memory must be taken into consideration.

This guide demonstrates several of the TileDB-VCF features for overcoming memory limitations when querying large datasets.&#x20;

## Setting the Memory Budget

One strategy for accommodating large queries is to simply increase the amount of memory available to `tiledbvcf`. By default `tiledbvcf` allocates 2GB of memory for queries. However, this value can be adjusted using the `memory_budget_mb` parameter. For the purposes of this tutorial the budget will be _decreased_ to demonstrate how `tiledbvcf` is able to perform genome-scale queries even in a memory constrained environment.

```python
import tiledbvcf
cfg = tiledbvcf.ReadConfig(memory_budget_mb=256)
ds = tiledbvcf.Dataset(uri, mode = "r", cfg = cfg)
```

## Performing Batched Reads

For queries that encompass many genomic regions you can simply provide an external `bed` file. In this example, you will query for any variants located in the promoter region of a known gene located on chromosomes 1-4.

After performing a query, you can use `read_completed()` to verify whether or not _all_ results were successfully returned.

```python
attrs = ["sample_name", "contig", "pos_start", "fmt_GT"]
df = ds.read(attrs, bed_file = "data/gene-promoters-hg38.bed")
ds.read_completed()

## False
```

In this case, it returned `False`, indicating the requested data was too large to fit into the allocated memory so `tiledbvcf` retrieved as many records as possible in this first batch. The remaining records can be retrieved using `continue_read()`. Here, we've setup our code to accommodate the possibility that the full set of results are split across multiple batches.

```python
print ("The dataframe contains")

while not ds.read_completed():
    print (f"\t...{df.shape[0]} rows")
    df = df.append(ds.continue_read())

print (f"\t...{df.shape[0]} rows")

## The dataframe contains
##   ...1525201 rows
##   ...3050402 rows
##   ...3808687 rows
```

Here is the final dataframe, which includes 3,808,687 records:

```python
df

##         sample_name contig  pos_start    fmt_GT
## 0       v2-Qhhvcspe   chr1          1  [-1, -1]
## 1       v2-YMaDHIoW   chr1          1  [-1, -1]
## 2       v2-Mcwmkqnx   chr1          1  [-1, -1]
## 3       v2-RzweTRSv   chr1          1  [-1, -1]
## 4       v2-ijrKdkKh   chr1          1  [-1, -1]
## ...             ...    ...        ...       ...
## 758280  v2-PDeVyHSO   chr4  190063262    [0, 0]
## 758281  v2-PDeVyHSO   chr4  190063264  [-1, -1]
## 758282  v2-PDeVyHSO   chr4  190063265  [-1, -1]
## 758283  v2-PDeVyHSO   chr4  190063392    [0, 0]
## 758284  v2-PDeVyHSO   chr4  190063418  [-1, -1]
## 
## [3808687 rows x 4 columns]
```

## Iteration

A Python generator version of the `read` method is also provided. This pattern provides a powerful interface for batch processing variant data.

```python
ds = tiledbvcf.Dataset(uri, mode = "r", cfg = cfg)

df = pd.DataFrame()
for batch in ds.read_iter(attrs, bed_file = "data/gene-promoters-hg38.bed"):
    df = df.append(batch, ignore_index = True)

df

##          sample_name contig  pos_start    fmt_GT
## 0        v2-Qhhvcspe   chr1          1  [-1, -1]
## 1        v2-YMaDHIoW   chr1          1  [-1, -1]
## 2        v2-Mcwmkqnx   chr1          1  [-1, -1]
## 3        v2-RzweTRSv   chr1          1  [-1, -1]
## 4        v2-ijrKdkKh   chr1          1  [-1, -1]
## ...              ...    ...        ...       ...
## 3808682  v2-PDeVyHSO   chr4  190063262    [0, 0]
## 3808683  v2-PDeVyHSO   chr4  190063264  [-1, -1]
## 3808684  v2-PDeVyHSO   chr4  190063265  [-1, -1]
## 3808685  v2-PDeVyHSO   chr4  190063392    [0, 0]
## 3808686  v2-PDeVyHSO   chr4  190063418  [-1, -1]
## 
## [3808687 rows x 4 columns]
```
