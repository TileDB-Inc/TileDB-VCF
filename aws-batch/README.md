# AWS Batch

This directory contains the files necessary to support VCF ingestion and export via [AWS Batch](https://aws.amazon.com/batch/).

Note: It's not necessary to use AWS Batch, or the scripts in this directory -- you can use the `tiledbvcf` executable directly to ingest VCF data from/to S3. The scripts here are just an AWS Batch interface over `tiledbvcf`.

## Batch ingestion overview

The `batch-ingest.py` script takes a list of BCF file URIs on S3 and a destination TileDB array URI and submits one or more ingestion jobs to AWS Batch.

To show usage info:

```bash
$ pip install -r requirements.txt
$ python3 batch-ingest.py --help
Usage: batch-ingest.py [OPTIONS]

  Ingest VCF sample data from multiple VCF/BCF files via AWS Batch.

Options:
  --array URI            S3 URI of destination TileDB array.  [required]
  --attributes CSV       Comma-separated list of VCF info and/or format field
                         names that should be extracted as separate TileDB
                         attributes. This option is only used if the
                         destination array does not yet exist.
  --samples PATH         Path to file containing a list of sample URIs (one
                         per line) to ingest.  [required]
  --metadata-s3 NAME     Name of a bucket that can be used for shared metadata
                         storage between batch jobs, e.g. uploading lists of
                         samples to register or ingest.  [required]
  --num-jobs N           Number of jobs to submit for ingestion. Each job will
                         be responsible for ingesting nsamples/njobs samples
                         to the array.
  --job-queue NAME       Name of job queue to use for jobs.  [required]
  --job-definition NAME  Name of job definition to use for jobs.  [required]
  --region NAME          AWS region name
  --help                 Show this message and exit.
```

**Examples**:

Ingest a list of BCF files on S3 into a new array on S3. The array will store the fields `GT`, `DP`, `GQ` and `MIN_DP` as separate TileDB attributes. The job definiton used is `my-job-defn` revision `3`, with the job queue `my-job-queue-name`. The bucket `bucket2-name` is used for inter-job shared metadata files:

```bash
$ cat samples.txt
s3://bucket-name/samples/1.bcf
s3://bucket-name/samples/2.bcf
...
$ python batch-ingest.py \
    --array s3://bucket-name/tiledb-array \
    --attributes GT,DP,GQ,MIN_DP \
    --samples samples.txt \
    --num-jobs 1 \
    --metadata-s3 my-md-bucket \
    --job-definition my-job-defn:3 \
    --job-queue my-job-queue-name
Submitted array creation job b95115d7-c992-4966-b886-a0fcf112c561
Submitted sample registration job e66d4917-baa3-4f03-bb4d-44c483ee3af4 using metadata s3://my-md-bucket/samples.txt
Submitted ingestion job(s) cc4f9e66-d6fe-4f54-b184-005037d0a200
```

You can now monitor the progress of the jobs given the printed job IDs.

## Batch export overview

The `batch-export.py` script takes an existing TileDB VCF array, a list of sample names to export, and a BED file, and submits one or more export jobs to AWS Batch.
 
To show usage info:

```bash
$ pip install -r requirements.txt
$ python3 batch-export.py --help
Usage: batch-export.py [OPTIONS]

  Exports BCF sample data from a TileDB array via AWS Batch.

Options:
  --array URI            S3 URI of TileDB array to read from.  [required]
  --dest-s3 URI          S3 URI where exported BCFs will be stored.
                         [required]
  --samples URI          S3 URI of file containing a list of sample names (one
                         per line) to export.  [required]
  --bed URI              S3 URI of BED file containing sample regions to
                         export.  [required]
  --job-queue NAME       Name of job queue to use for jobs.  [required]
  --job-definition NAME  Name of job definition to use for jobs.  [required]
  --attributes CSV       Comma-separated list of BCF info and/or format field
                         names that should be included in the exported BCFs.
  --num-jobs N           Number of jobs to submit for export. Each job will be
                         responsible for exporting nsamples/njobs samples from
                         the array.
  --region NAME          AWS region name
  --help                 Show this message and exit.
```

**Examples**:

Export 10 samples from an array using a BED file with 1000 regions.

The job definition used is `my-job-defn` revision `3`, with the job queue `my-job-queue-name`. The bucket `my-bucket` is used to store the exported BCFs:

```bash
python batch-export.py \
    --array s3://bucket-name/tiledb-array \
    --samples s3://bucket-name/10-sample-names.txt \
    --bed s3://bucket-name/1000-regions.bed \
    --dest-s3 s3://my-bucket/output-bcfs \
    --num-jobs 1 \
    --job-definition my-job-defn:3 \
    --job-queue my-job-queue-name
Submitted export job(s) cc4f9e66-d6fe-4f54-b184-005037d0a200
```

You can now monitor the progress of the jobs given the printed job IDs.

## Setting up AWS Batch

There are several steps to getting a `tiledbvcf` environment set up for AWS Batch, assuming you have already set up a Batch compute environment.

1. Create an AMI with a local EBS scratch volume mounted.
2. Build and deploy the Docker image to ECR
3. Create a `tiledbvcf` job definition specifying the Docker image
4. Create a job queue.

The steps are documented in detail below.

### 1. Create the AMI

The ingestion and export process with `tiledbvcf` can require a large amount of local scratch space to store the BCF/VCF files.

While creating a custom AMI is outside of the scope of this guide, the AMI should have the following parameters:

- Derived from Amazon ECS-Optimized Amazon Linux AMI)
- Large (e.g. 1 TB) EBS mounted at a fixed+known location, with 0777 permissions. This volume
  will be for transient data only, so delete on termination should be enabled.

The Batch compute environment used for the ingestion jobs should use this AMI. The `batch-ingest.py`/`batch-export.py` scripts will be responsible for passing the scratch volume path and size to `tiledbvcf`.

### 2. Deploy the Docker image

To build the Docker image:

```bash
$ cd libtiledbvcf
$ docker build -f aws-batch/docker/Dockerfile -t tiledb-genomics .
```

To be usable in AWS Batch, push the docker image to ECR, e.g.:

```bash
$ aws --region us-east-1 ecr create-repository --repository-name tiledb-genomics
{
    "repository": {
        "registryId": "<reg-id>",
        "repositoryName": "tiledb-genomics",
        "repositoryArn": "arn:aws:ecr:us-east-1:<reg-id>:repository/tiledb-genomics",
        "createdAt": 1543352113.0,
        "repositoryUri": "<reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledb-genomics"
    }
}
$ $(aws ecr get-login --no-include-email --region us-east-1)
$ docker tag tiledb-genomics:latest <reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledb-genomics:latest
$ docker push <reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledb-genomics:latest
```

Where `<reg-id>` refers to the value of `registryId`.

### 3. Create the job definition

The Batch jobs require read/write access to S3, so you should specify the corresponding IAM role in `container-properties` when creating the job definition if necessary. The following definition specifies 16 vCPUs and 64 GB of memory should be available to jobs, and a 24 hour timeout.

```bash
aws --region us-east-1 batch register-job-definition \
  --job-definition-name tilevcf-24h \
  --type container \
  --timeout '{"attemptDurationSeconds": 86400}' \
  --container-properties '{"image": "<reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledb-genomics:latest", "vcpus": 16, "memory": 65536, "volumes": [{"host": {"sourcePath": "/data"}, "name": "data"}], "mountPoints": [{"containerPath": "/data", "readOnly": false, "sourceVolume": "data"}]}'
```

The mount point path used in this job definition must match the `scratch_path` path used in `batch-ingest.py`.

### 4. Create the job queue

First get the ARN of the compute environment you wish to use by noting the value of `computeEnvironmentArn`:
```bash
$ aws --region us-east-1 batch describe-compute-environments
...
```

Next, create the job queue:

```bash
$ aws --region us-east-1 batch create-job-queue \
  --job-queue-name tilevcf-ingestion-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order '[{"computeEnvironment": "<compute-env-arn>", "order": 1}]'
```

You should now be able to launch batch jobs with `batch-ingest.py` and `batch-export.py`.