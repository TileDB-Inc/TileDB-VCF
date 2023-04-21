# Work with Cloud Object Stores

TileDB Embedded provides native support for reading from and writing to cloud object stores like AWS S3, Google Cloud Storage, and Microsoft Azure Blob Store. This guide will cover some considerations for using TileDB-VCF with these services. The examples will focus exclusively on S3, which is the most widely used, but note any of the aforementioned services can be substituted, as well as on-premise services like MinIO that provide S3-compatible APIs.

## Remote Datasets

The process of creating a TileDB-VCF dataset on S3 is nearly identical to creating a local dataset. The only difference being an `s3://` address is passed to the `--uri` argument rather than a local file path.

```bash
tiledbvcf create --uri s3://my-bucket/my_dataset
```

This also works when querying a TileDB-VCF dataset located on S3.

```bash
tiledbvcf export \
  --uri s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20 \
  --sample-names v2-tJjMfKyL,v2-eBAdKwID \
  -Ot --tsv-fields "CHR,POS,REF,S:GT" \
  --regions "chr7:144000320-144008793,chr11:56490349-56491395"
```

## Remote VCF Files

VCF files located on S3 can be ingested directly into a TileDBVCF dataset using 1 of 2 different possible approaches.&#x20;

### Direct Ingestion

The first approach is the easiest, you simply pass the `tiledbvcf store` command a list of S3 URIs and TileDB-VCF takes care of the rest:

```bash
tiledbvcf store \
    --uri my_dataset \
    s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G4.bcf
```

In this approach, remote VCF _index_ files (which are relatively tiny) are downloaded locally, allowing TileDB-VCF to retrieve chunks of variant data from the remote VCF files without having to download them in full. By default, index files are downloaded to your current working directory, however, you can choose to store them in different location (e.g., a temporary directory) using the `--scratch-dir` argument.

### Batched Downloading

The second approach is to download batches of VCF files in their entirety before ingestion, which may slightly improve ingestion performance.  This approach requires allocating TileDB-VCF with scratch disk space using the `--scratch-mb` and `--scratch-dir` arguments.

```bash
tiledbvcf store \
    --uri my_dataset \
    --scratch-dir "$TMPDIR" \
    --scratch-mb 4096 \
    s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G4.bcf
```

The number of VCF files that are downloaded at a time is determined by the `--sample-batch-size` parameter, which defaults to 10. Downloading and ingestion happens asynchronously, so, for example, batch 3 will be downloaded as batch 2 is being ingestion. As a result, you must configure enough scratch space to store at least 20 samples, assuming a batch size of 10.

## Authentication

For TileDB to access a remote storage bucket you must be properly authenticated on the machine running TileDB. For S3, this means having access to the appropriate AWS [access key ID and secret access key](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds). This typically happens in one of three ways:

### 1. Using the AWS CLI

If the AWS Command Line Interface (CLI) is installed on your machine,  running [`aws configure`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-creds) will store your credentials in a local profile that TileDB can access. You can verify the CLI has been previously configured by running:

```bash
aws s3 ls
```

If properly configured, this will output a list of the S3 buckets you (and thus TileDB) can access.

### 2. Using Configuration Parameters

You can pass your AWS access key ID and secret access key to TileDB-VCF directly via the `--tiledb-config` argument, which expects a comma-separated string:

```bash
tiledbvcf store \
    --uri my_dataset \
    --tiledb-config vfs.s3.aws_access_key_id=<id>,vfs.s3.aws_secret_access_key=<secret> \
    s3://tiledb-inc-demo-data/examples/notebooks/vcfs/G4.bcf
```

### 3. Using Environment Variables

Your AWS credentials can also be passed to TileDB by defining the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.
