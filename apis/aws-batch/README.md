# TileDB-VCF with AWS Batch

This directory contains the files to run TileDB-VCF ingestion and export via [AWS Batch](https://aws.amazon.com/batch/).

The AWS Batch integration simply runs the `tiledbvcf` command-line tool inside a container running on instances belonging to an AWS Batch compute environment. Distributing the work of ingestion (or export) is done simply by partitioning the samples being ingested (or exported) across a configurable number of jobs running on the Batch instances.

These instructions assume a basic familiarity with the concepts of AWS Batch such as compute environments, job definitions, etc. The [AWS Batch user guide](https://docs.aws.amazon.com/batch/latest/userguide/Batch_GetStarted.html) serves as a good reference on these concepts.

## Setting up AWS Batch

There are several high-level steps to getting a TileDB-VCF environment set up for AWS Batch:

1. Create a custom AMI with a local EBS scratch volume mounted.
2. Build and deploy the `tiledbvcf` Docker image to ECR.
3. Create an AWS Batch compute environment using the custom AMI.
4. Create a `tiledbvcf` job definition specifying the Docker image.
5. Create a job queue.

These steps are documented in detail below.

### 1. Create the AMI

The ingestion and export process with `tiledbvcf` can require a large amount of local scratch space to store the BCF/VCF files. The AMI you create should have a pre-mounted scratch volume somewhere accessible inside the container.

While creating a custom AMI in general is outside of the scope of this guide, here is a rough outline.

1. Create a new EC2 instance using the AWS-provided AMI `Amazon ECS-Optimized Amazon Linux 2 AMI` (instance type does not matter).
2. In the "Add Storage" step, add a large (e.g. 1TB) EBS volume. **Make sure to enable "Delete on Termination"**.
3. Launch the instance and SSH into it. Then run the following commands:
    ```
    sudo yum -y update
    sudo mkfs -t ext4 /dev/nvme1n1
    sudo mkdir /data
    sudo echo -e '/dev/nvme1n1\t/data\text4\tdefaults\t0\t0' | sudo tee -a /etc/fstab
    sudo mount -a
    sudo chmod 0777 /data
    sudo systemctl stop ecs
    sudo rm -rf /var/lib/ecs/data/ecs_agent_data.json
    ```
4. Log out of the instance, and create an AMI from it (while still running).
5. Terminate the instance, as it is no longer needed.

**Make sure you have enabled "Delete on Termination" for the extra EBS volume.**

The AWS Batch compute environment for TileDB-VCF will use this AMI. The `batch-ingest.py`/`batch-export.py` scripts are responsible for using the correct the EBS volume path (`/data`) and size with the `tiledbvcf` CLI.

### 2. Build and deploy the Docker image

To build the TileDB-VCF Docker image containing the CLI:

```bash
$ cd TileDB-VCF/
$ docker build -f docker/Dockerfile-cli -t tiledbvcf-cli libtiledbvcf
```

To be usable in AWS Batch, you must then push the docker image to ECR, e.g.:

```bash
$ aws --region us-east-1 ecr create-repository --repository-name tiledbvcf
{
    "repository": {
        "registryId": "<reg-id>",
        "repositoryName": "tiledbvcf",
        "repositoryArn": "arn:aws:ecr:us-east-1:<reg-id>:repository/tiledbvcf",
        "createdAt": 1543352113.0,
        "repositoryUri": "<reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledbvcf"
    }
}
$ $(aws ecr get-login --no-include-email --region us-east-1)
$ docker tag tiledbvcf:latest <reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledbvcf:latest
$ docker push <reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledbvcf:latest
```

Where `<reg-id>` refers to the value of `registryId`.

### 3. Create the compute environment

On the AWS web console, find the Batch service and click "Create compute environment". Here are some example configuration choices:
- Allowed instance types: `m5.4xlarge`
- Minimum vCPUs: 0
- Desired vCPUs: 0
- Maximum vCPUs: 512

Check the "Enable user-specified AMI ID" box and paste the AMI ID of the AMI you created in step 1.

### 4. Create the job definition

The following definition specifies 16 vCPUs and 64 GB of memory should be available to jobs, and a 24 hour timeout.

```bash
aws --region us-east-1 batch register-job-definition \
  --job-definition-name tiledbvcf-24h \
  --type container \
  --timeout '{"attemptDurationSeconds": 86400}' \
  --container-properties '{"image": "<reg-id>.dkr.ecr.us-east-1.amazonaws.com/tiledbvcf:latest", "vcpus": 16, "memory": 65536, "volumes": [{"host": {"sourcePath": "/data"}, "name": "data"}], "mountPoints": [{"containerPath": "/data", "readOnly": false, "sourceVolume": "data"}]}'
```

Set the `image` value to the ID of the image you created in step 2. Note that the mount point path (`/data`) used in this job definition is what thethe `scratch_path` value should be set to in `batch-ingest.py`.

Next, using the AWS web console, create an IAM role the tasks will inherit to access S3:
- Service using role: "Elastic Container Service"
- Use case: "Elastic Container Service Task"
- Attached policy: "AmazonS3FullAccess"
Give the role a descriptive name such as `tiledb-vcf-batch-s3-access`.

Finally, find the job definition in the web console (in the Batch service). Create a new revision of the job definition, and attach the IAM role.

### 5. Create the job queue

Create the job queue, replacing `<>` with the ARN of the compute environment you created in step 3.

```bash
aws --region us-east-1 batch create-job-queue \
  --job-queue-name tiledbvcf-job-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order '[{"computeEnvironment": "<compute-env-arn>", "order": 1}]'
```

You should now be ready to launch batch jobs with `batch-ingest.py` and `batch-export.py`. Both of these scripts take the job queue and definition names as required arguments. Each script is detailed below.

## Batch ingestion overview

The `batch-ingest.py` script takes a list of BCF file URIs on S3 and a destination URI for the TileDB-VCF dataset and submits one (or more) ingestion jobs to AWS Batch.

To show usage info:

```bash
$ pip install -r requirements.txt
$ python3 batch-ingest.py --help
Usage: batch-ingest.py [OPTIONS]

  Ingest VCF sample data into a TileDB-VCF dataset via AWS Batch.

  This script requires an existing AWS Batch setup, including a job
  definition with a Docker image containing the TileDB-VCF CLI executable.
  The CLI executable is invoked in each batch job by this script using the
  `command` container override parameter.

  Ingestion of the samples (specified via --samples argument) is distributed
  across the specified number of Batch jobs (--num-jobs), irrespective of
  the number of instances in the compute environment. For example, if there
  are 1,000 samples to be ingested, the compute environment contains 10
  instances, and 10 jobs are requested, each instance will execute 1 job
  that ingests 100 samples at a time. If 100 jobs were requested, 100 jobs
  would be queued (each ingesting 10 samples), and the 10 instances would
  pull jobs from the queue until all are finished.

  The bucket specified with the --metadata-s3 argument is used to store a
  file for each job containing the list of samples the job should ingest.

Options:
  --dataset-uri URI      S3 URI of destination TileDB-VCF dataset. If the
                         dataset does not exist, it will be created.
                         [required]
  --samples PATH         Path to file containing a list of sample URIs (one
                         per line) to ingest.  [required]
  --metadata-s3 NAME     Name of a bucket that can be used for shared metadata
                         storage between batch jobs, e.g. uploading lists of
                         samples to register or ingest.  [required]
  --job-queue NAME       Name of job queue to use for jobs.  [required]
  --job-definition NAME  Name of job definition to use for jobs.  [required]
  --attributes CSV       Comma-separated list of VCF info and/or format field
                         names that should be extracted as separate TileDB
                         attributes. This option is only used if the
                         destination array does not yet exist.
  --num-jobs N           Number of jobs to submit for ingestion. Each job will
                         be responsible for ingesting nsamples/njobs samples
                         to the array. When combined with --incremental, each
                         incremental batch uses this many jobs.
  --region NAME          AWS region name of Batch environment
  --retries N            Max number (1-10) of retries for failed jobs.
  --wait                 Waits for all jobs to complete before exiting.
  --incremental N        If specified, ingest the samples in N batches instead
                         of all at once.
  --help                 Show this message and exit.
```

**Examples**:

Ingest a list of BCF files on S3 into a new array on S3. The bucket `bucket2-name` is used for inter-job shared metadata files:

```bash
$ cat samples.txt
s3://bucket-name/samples/1.bcf
s3://bucket-name/samples/2.bcf
...
$ python batch-ingest.py \
    --dataset-uri s3://my-bucketname/test-vcf-dataset \
    --samples samples.txt \
    --metadata-s3 bucket2-name \
    --job-queue tiledbvcf-job-queue \
    --job-definition tiledbvcf-24h \
    --num-jobs 10
Submitted array creation job 52ea520a-730b-4421-9512-e821fdfe7748
Submitted sample registration job aa965cdb-3bfd-44c2-a8b0-1b3ffca4ae40 using metadata s3://tyler-vcf-batch-md/0.tmpcdcw6dt7.txt
Submitted ingestion job(s) 56340c50-e3b7-45f1-bf7e-1f67115cf26c 9b0c8d6a-b79e-45a6-9b4a-bf93c8cb4257 c7681f1e-5ab8-418d-964d-2dfc252783b0 dd4b44a1-c935-454b-b66b-55ff13a652f0 d0b7bcfc-ea6e-4fe2-b238-1c6d4ce8b5ae 9baca5d1-24ff-494d-92a5-3594d72cfb98 ba4233d1-43ac-49b5-bdda-e1937a73e241 fd40eb62-5774-4996-8774-c7d99918fc7e 6b21ccab-13da-46c4-bf9a-ddffece7bd2d e6dcf53e-9e7f-409f-8c06-1ceeda89c78e
```

You can now monitor the progress of the jobs given the printed job IDs. By passing the `--wait` flag, the `batch-ingest.py` script will not exit until all jobs have terminated (with success or error).

## Batch export overview

The `batch-export.py` script takes an existing TileDB-VCF dataset, a list of sample names to export, and a BED file, and submits one or more export jobs to AWS Batch. The export process produces one BCF output file per exported sample.
 
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

Export 10 samples from an array using a BED file with 1,000,000 regions.

```bash
$ cat 10-names.txt
SampleName1
SampleName2
...
$ aws s3 cp 10-names.txt s3://my-bucketname/10-names.txt
$ python batch-export.py \
    --dataset-uri s3://my-bucketname/test-vcf-dataset \
    --dest-s3 s3://my-bucketname/bcf-exports/ \
    --samples s3://my-bucketname/10-names.txt \
    --bed s3://my-bucketname/bedfiles/sorted1000000.bed \
    --job-queue tiledbvcf-job-queue \
    --job-definition tiledbvcf-24h \
    --num-jobs 2
Submitted export job(s) cf3dcdd6-c99b-4910-80f6-1161fb1e21a4 da4b7081-5046-46bd-bc19-2278ed949ea5
```

You can now monitor the progress of the jobs given the printed job IDs.

## Getting logs from failed jobs

If a job fails, use the job ID to get the name of the output log stream, e.g.:
```bash
$ aws --region us-east-1 batch describe-jobs --jobs 52ea520a-730b-4421-9512-e821fdfe7748 | grep logStream
                          "logStreamName": "tiledbvcf-24h/default/30ed4636-10bd-4105-9c4f-e90e91f86ec8",
                  "logStreamName": "tiledbvcf-24h/default/30ed4636-10bd-4105-9c4f-e90e91f86ec8",
```

Then you can use the log stream name to get the log events from the failed job, e.g:
```bash
$ aws --region us-east-1 logs get-log-events --log-group-name /aws/batch/job --log-stream-name tiledbvcf-24h/default/30ed4636-10bd-4105-9c4f-e90e91f86ec8 | grep message
            "message": "[2019-10-08 13:18:15.246] [tiledb] [Process: 1] [Thread: 1] [error] [TileDB::S3] Error: Error while listing with prefix 's3://my-bucketname/test-vcf-dataset/' and delimiter '/'",
            "message": "Exception:  AccessDenied",
            "message": "Error message:  Access Denied with address : 52.216.186.147",
            "message": "terminate called after throwing an instance of 'tiledb::TileDBError'",
            "message": "  what():  [TileDB::S3] Error: Error while listing with prefix 's3://my-bucketname/test-vcf-dataset/' and delimiter '/'",
            "message": "Exception:  AccessDenied",
            "message": "Error message:  Access Denied with address : 52.216.186.147",
```