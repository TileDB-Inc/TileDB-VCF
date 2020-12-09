#!/usr/bin/env python

import batchmetrics
import boto3
import click
import math
import sys
import tempfile
import os

# Path and size of scratch directory in the tiledbvcf container.
scratch_path = "/data"
scratch_size_mb = 1000 * 1024  # 1TB


class JobInfo(object):
    """Simple struct to contain batch job information."""

    def __init__(
        self,
        client=None,
        job_queue="",
        job_definition="",
        depends_on=None,
        max_retries=1,
        metadata_s3="",
        num_store_jobs=1,
    ):
        self.client = client
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.max_retries = max_retries
        self.num_store_jobs = num_store_jobs
        self.metadata_s3 = metadata_s3
        if depends_on is not None:
            self.depends_on = depends_on
        else:
            self.depends_on = []

    def depends_on_as_dict(self):
        """Returns the job dependencies list in a form that AWS can use."""
        result = []
        for job_id in self.depends_on:
            result.append({"jobId": job_id, "type": "SEQUENTIAL"})
        return result


def upload_to_s3(local_file, s3_bucket):
    """Upload the given local file to the given S3 bucket."""
    s3 = boto3.resource("s3")
    s3.meta.client.upload_file(local_file, s3_bucket, os.path.basename(local_file))
    return "s3://" + s3_bucket + "/" + os.path.basename(local_file)


def upload_sample_batches(sample_batches, s3_bucket):
    """Creates and uploads one or more sample batch text files."""
    uris = []
    for i, batch in enumerate(sample_batches):
        with tempfile.TemporaryDirectory() as tmpdir:
            basename = os.path.basename(tmpdir)
            tmpfile = os.path.join(tmpdir, "samples_batch{}.{}.txt".format(i, basename))
            with open(tmpfile, "w") as f:
                for line in batch:
                    f.write(line)
                    f.write("\n")
            uris.append(upload_to_s3(tmpfile, s3_bucket))
    return uris


def get_sample_batches(samples_file, num_batches):
    """Return a list of sample batches.

    This returns a list containing num_batches lists of sample URIs.
    """
    all_lines = []
    with open(samples_file, "r") as f:
        for line in f:
            line = line.strip()
            if len(line) > 0:
                all_lines.append(line)
    num_samples = len(all_lines)
    samples_per_batch = int(math.ceil(num_samples / float(num_batches)))
    batches = [
        all_lines[i : i + samples_per_batch]
        for i in range(0, num_samples, samples_per_batch)
    ]
    return batches


def get_incremental_samples_file(all_samples_file, i, num_partitions, tmpdir):
    """Gets the ith incremental ingest portion of the given samples file.

    Partitions the samples in the given file into the ith partition and
    write the ith partition to a new file.
    """
    with open(all_samples_file, "r") as in_f:
        lines = in_f.readlines()
        num_lines = len(lines)
        lines_per_part = int(math.ceil(num_lines / float(num_partitions)))
        start = i * lines_per_part
        end = min(num_lines, start + lines_per_part)

        lines = lines[start:end]
        if len(lines) == 0:
            return None

        basename = os.path.basename(tmpdir)
        path = os.path.join(tmpdir, "{}.{}.txt".format(i, basename))
        with open(path, "w") as out_f:
            out_f.writelines(lines)
        return path


def create_array(array_uri, attributes, job_info):
    """Submit a Batch job to create the TileDB array."""
    tilevcf_args = ["create", "-u", array_uri, "-a", attributes]
    # Configure the job requirements
    nvcpus = 16
    mem_req_mb = 60 * 1024
    response = job_info.client.submit_job(
        jobName="create_array",
        jobQueue=job_info.job_queue,
        jobDefinition=job_info.job_definition,
        dependsOn=job_info.depends_on_as_dict(),
        containerOverrides={
            "vcpus": nvcpus,
            "memory": mem_req_mb,
            "command": tilevcf_args,
        },
    )
    job_id = response["jobId"]
    print("Submitted array creation job {}".format(job_id))
    return job_id


def register_samples(array_uri, samples_file, job_info):
    """Submit a Batch job to register samples in the array before ingestion."""
    samples_uri = upload_to_s3(samples_file, job_info.metadata_s3)
    tilevcf_args = [
        "register",
        "-u",
        array_uri,
        "-d",
        scratch_path,
        "-s",
        str(scratch_size_mb),
        "-f",
        samples_uri,
    ]
    # Configure the job requirements
    nvcpus = 16
    mem_req_mb = 60 * 1024
    response = job_info.client.submit_job(
        jobName="register_samples",
        jobQueue=job_info.job_queue,
        jobDefinition=job_info.job_definition,
        dependsOn=job_info.depends_on_as_dict(),
        containerOverrides={
            "vcpus": nvcpus,
            "memory": mem_req_mb,
            "command": tilevcf_args,
        },
    )
    job_id = response["jobId"]
    print(
        "Submitted sample registration job {} using metadata {}".format(
            job_id, samples_uri
        )
    )
    return job_id


def ingest_samples(array_uri, samples_file, job_info):
    """Submit multiple Batch jobs to ingest all samples into the array."""
    # Split up the work and upload the batches of sample URIs.
    samples_per_job = get_sample_batches(samples_file, job_info.num_store_jobs)
    sample_batch_uris = upload_sample_batches(samples_per_job, job_info.metadata_s3)

    # Configure the job requirements. Ingestion does not really benefit from
    # packing multiple jobs onto the same instance, so these requirements are
    # calibrated to use a full m5.4xlarge instance per job.
    nvcpus = 16
    mem_req_mb = 60000
    tilevcf_mem_gb = int(mem_req_mb / 1024)

    # Submit one job per sample batch.
    job_ids = []
    for batch_uri in sample_batch_uris:
        tilevcf_args = [
            "store",
            "-u",
            array_uri,
            "-d",
            scratch_path,
            "-s",
            str(scratch_size_mb),
            "-t",
            str(nvcpus),
            "-f",
            batch_uri,
            "--remove-sample-file",
        ]
        response = job_info.client.submit_job(
            jobName="ingest_samples",
            jobQueue=job_info.job_queue,
            jobDefinition=job_info.job_definition,
            dependsOn=job_info.depends_on_as_dict(),
            retryStrategy={"attempts": job_info.max_retries},
            containerOverrides={
                "vcpus": nvcpus,
                "memory": mem_req_mb,
                "command": tilevcf_args,
            },
        )
        job_ids.append(response["jobId"])

    print("Submitted ingestion job(s) {}".format(" ".join(job_ids)))
    return job_ids


@click.command()
@click.option(
    "--dataset-uri",
    required=True,
    help="S3 URI of destination TileDB-VCF dataset. If the dataset "
    "does not exist, it will be created.",
    metavar="URI",
)
@click.option(
    "--samples",
    required=True,
    help="Path to file containing a list of sample URIs (one per" " line) to ingest.",
    metavar="PATH",
)
@click.option(
    "--metadata-s3",
    required=True,
    help="Name of a bucket that can be used for shared metadata "
    "storage between batch jobs, e.g. uploading lists of "
    "samples to register or ingest.",
    metavar="NAME",
)
@click.option(
    "--job-queue",
    required=True,
    help="Name of job queue to use for jobs.",
    metavar="NAME",
)
@click.option(
    "--job-definition",
    required=True,
    help="Name of job definition to use for jobs.",
    metavar="NAME",
)
@click.option(
    "--attributes",
    help="Comma-separated list of VCF info and/or format field names "
    "that should be extracted as separate TileDB attributes. "
    "This option is only used if the destination array does not "
    "yet exist.",
    default="fmt_GT,fmt_DP,fmt_GQ,fmt_MIN_DP",
    metavar="CSV",
)
@click.option(
    "--num-jobs",
    default=1,
    help="Number of jobs to submit for ingestion. Each job will be "
    "responsible for ingesting nsamples/njobs samples to the "
    "array. When combined with --incremental, each incremental "
    "batch uses this many jobs.",
    metavar="N",
)
@click.option(
    "--region",
    help="AWS region name of Batch environment",
    default="us-east-1",
    metavar="NAME",
)
@click.option(
    "--retries",
    help="Max number (1-10) of retries for failed jobs.",
    default=1,
    metavar="N",
)
@click.option(
    "--wait", help="Waits for all jobs to complete before exiting.", is_flag=True
)
@click.option(
    "--incremental",
    default=1,
    metavar="N",
    help="If specified, ingest the samples in N batches instead of " "all at once.",
)
def main(
    dataset_uri,
    samples,
    attributes,
    job_queue,
    job_definition,
    region,
    metadata_s3,
    num_jobs,
    retries,
    wait,
    incremental,
):
    """Ingest VCF sample data into a TileDB-VCF dataset via AWS Batch.

    This script requires an existing AWS Batch setup, including a job definition
    with a Docker image containing the TileDB-VCF CLI executable. The CLI
    executable is invoked in each batch job by this script using the `command`
    container override parameter.

    Ingestion of the samples (specified via --samples argument) is distributed
    across the specified number of Batch jobs (--num-jobs), irrespective of
    the number of instances in the compute environment. For example, if there
    are 1,000 samples to be ingested, the compute environment contains 10
    instances, and 10 jobs are requested, each instance will execute 1 job that
    ingests 100 samples at a time. If 100 jobs were requested, 100 jobs would be
    queued (each ingesting 10 samples), and the 10 instances would pull jobs
    from the queue until all are finished.

    The bucket specified with the --metadata-s3 argument is used to store a file
    for each job containing the list of samples the job should ingest.
    """

    job_info = JobInfo(
        job_queue=job_queue,
        job_definition=job_definition,
        client=boto3.client("batch", region_name=region),
        max_retries=retries,
        num_store_jobs=num_jobs,
        metadata_s3=metadata_s3,
    )
    create_job_id = create_array(dataset_uri, attributes, job_info)

    for i in range(0, incremental):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch_samples = get_incremental_samples_file(
                samples, i, incremental, tmpdir
            )

            # Check for empty list of samples
            if batch_samples is None:
                continue

            job_info.depends_on = [create_job_id] if i == 0 else []
            register_job_id = register_samples(dataset_uri, batch_samples, job_info)

            job_info.depends_on = [register_job_id]
            job_ids = ingest_samples(dataset_uri, batch_samples, job_info)

            if wait:
                # Registration stats
                name = "registration {} of {}".format(i + 1, incremental)
                stats = batchmetrics.wait_all(
                    [register_job_id], job_info.client, job_name=name
                )
                batchmetrics.print_stats_report(stats, instance_price_per_hr=0.768)

                # Ingest stats
                name = "ingestion {} of {}".format(i + 1, incremental)
                stats = batchmetrics.wait_all(job_ids, job_info.client, job_name=name)
                batchmetrics.print_stats_report(stats, instance_price_per_hr=0.768)
                if stats["num_succeeded"] != stats["num_jobs"]:
                    sys.exit(1)


if __name__ == "__main__":
    main()
