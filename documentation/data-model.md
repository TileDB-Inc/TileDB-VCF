# Data Model

[The Solution](the-solution.md) section provides a high-level overview of _how_ and _why_ TileDB-VCF uses 3D sparse arrays to store genomic variant data. What follows are the technical implementation details about the underlying arrays, including their schemas, dimensions, tiling order, attributes, metadata.

## TileDB-VCF Dataset

A TileDB-VCF dataset is composed of a group of two or more separate TileDB arrays: 

* a 3D sparse array for the actual genomic variants and associated fields/attributes
* a 1D sparse array for the metadata stored in each single-sample VCF header

## Data Array

### Basic schema parameters

| **Parameter** | **Value** |
| :--- | :--- |
| Array type | Sparse |
| Rank | 3D |
| Cell order | Row-major |
| Tile order | Row-major |

### Dimensions

The dimensions in the schema are:

| Dimension Name | TileDB Datatype | Corresponding VCF Field |
| :--- | :--- | :--- |
| `contig` | `TILEDB_STRING_ASCII` | `CHR` |
| `start_pos` | `uint32_t` | VCF`POS`plus TileDB anchors |
| `sample` | `TILEDB_STRING_ASCII` | Sample name |

As mentioned before, the coordinates of the 3D array are `contig` along the first dimension, chromosomal location of the variants start position along the second dimension, and `sample` names along the third dimension.

### Attributes

For each field in a single-sample VCF record there is a corresponding attribute in the schema.

| Attribute Name | TileDB Datatype | Description |
| :--- | :--- | :--- |
| `end_pos` | `uint32_t` | VCF `END` position of VCF records |
| `qual` | `float` | VCF `QUAL` field |
| `alleles` | `var<char>` | CSV list of `REF` and `ALT` VCF fields |
| `id` | `var<char>` | VCF `ID` field |
| `filter_ids` | `var<int32_t>` | Vector of integer IDs of entries in the `FILTER` VCF field |
| `real_start_pos` | `uint32_t` | VCF `POS`\(no anchors\) |
| `info` | `var<uint8_t>` | Byte blob containing any `INFO` fields that are not stored as explicit attributes |
| `fmt` | `var<uint8_t>` | Byte blob containing any `FMT` fields that are not stored as explicit attributes |
| `info_*` | `var<uint8_t>` | One or more attributes storing specific VCF `INFO` fields, e.g. `info_DP`, `info_MQ`, etc. |
| `fmt_*` | `var<uint8_t>` | One or more attributes storing specific VCF `FORMAT` fields, e.g. `fmt_GT`, `fmt_MIN_DP`, etc. |

The `info_*` and `fmt_*` attributes allow individual `INFO` or `FMT` VCF fields to be extracted into explicit array attributes. This can be beneficial if your queries frequently access only a subset of the `INFO` or `FMT` fields, as no unrelated data then needs to be fetched from storage.

::: {.callout-note}
The choice of which fields to extract as explicit array attributes is user-configurable during array creation.
:::

Any extra info or format fields not extracted as explicit array attributes are stored in the byte blob attributes, `info` and `fmt`. 

###  Metadata

* `anchor_gap` Anchor gap value
* `extra_attributes` List of `INFO` or `FMT` field names that are stored as explicit array attributes
* `version` Array schema version

These metadata values are updated during array creation, and are used during the export phase. The metadata is stored as "array metadata" in the sparse `data` array.

::: {.callout-warning}
When ingesting samples, the _sample header_ must be identical for all samples with respect to the contig mappings. That means all samples must have the exact same set of contigs listed in the VCF header. This requirement will be relaxed in future versions.
{% endhint %}

## VCF Headers Array

The `vcf_headers` array stores the original text of every ingested VCF header in order to:

1. ensure the original VCF file can be fully recovered for any given sample
2. reconstruct an `htslib` header instance when reading from the dataset, which is used for operations such as mapping a filter ID back to the filter string, etc.

### Basic schema parameters

| Parameter | Value |
| :--- | :--- |
| Array type | Sparse |
| Rank | 1D |
| Cell order | Row-major |
| Tile order | Row-major |

### Dimensions

| Dimension Name | TileDB Datatype | Description |
| :--- | :--- | :--- |
| `sample` | `TILEDB_STRING_ASCII` | Sample name |

### Attributes

| Attribute Name | TileDB Datatype | Description |
| :--- | :--- | :--- |
| `header` | `var<char>` | Original text of the VCF header |

## Putting It All Together

To summarize, we've described three main entities:

* The variant data array \(3D sparse\)
* The general metadata, stored in the variant data array as metadata
* The VCF header array \(1D sparse\)

All together we term this a "TileDB-VCF dataset." Expressed as a directory hierarchy, a TileDB-VCF dataset has the following structure:

```text
<dataset_uri>/
  |_ __tiledb_group.tdb
  |_ data/
      |_ __array_schema.tdb
      |_ __meta/
            |_ <general-metadata-here>
      ... <other array directories/fragments and files>
  |_ vcf_headers/
      |_ __array_schema.tdb
      ... <other array directories/fragments and files>
```

The root of the dataset, `<dataset_uri>` is a TileDB group. The `data` member is the TileDB 3D sparse array storing the variant data. This array stores the general TileDB-VCF metadata as its array metadata in folder `data/__meta`. The `vcf_headers` member is the TileDB 1D sparse array containing the VCF header data.

## Configurable Parameters

During _array creation_, there are several array-related parameters that the user can control. These are:

* Array data tile capacity \(default 10,000\)
* The "anchor gap" size \(default 1,000\)
* The list of `INFO` and `FMT` fields to store as explicit array attributes \(default is none\).

Once chosen, these parameters _cannot be changed_.

During _sample ingestion_, the user can specify the:

* Sample batch size \(default 10\)

The above parameters may impact read and write performance, as well as the size of the persisted array. Therefore, some care should be taken to determine good values for these parameters before ingesting a large amount of data into an array. 

