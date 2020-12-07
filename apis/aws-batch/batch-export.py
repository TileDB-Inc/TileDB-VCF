#!/usr/bin/env python

import batchmetrics
import boto3
import click
import sys

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
        num_export_jobs=1,
    ):
        self.client = client
        self.job_queue = job_queue
        self.job_definition = job_definition
        self.max_retries = max_retries
        self.num_export_jobs = num_export_jobs
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


def export_samples(array_uri, samples_uri, bed_uri, dest_s3, config_dict, job_info):
    """Submit multiple Batch jobs to export samples from the array."""
    # Configure the job requirements. Export benefits from packing multiple
    # processes onto a single node.
    nvcpus = 4
    mem_req_mb = 15000

    # Submit the configured number of jobs, specifying the partition to tiledbvcf.
    job_ids = []
    for i in range(0, job_info.num_export_jobs):
        tilevcf_args = [
            "export",
            "-u",
            array_uri,
            "-R",
            bed_uri,
            "-f",
            samples_uri,
            "-d",
            scratch_path,
            "--upload-dir",
            dest_s3,
            "--mem-budget-mb",
            "1500",
            "--sample-partition",
            "{}:{}".format(i, job_info.num_export_jobs),
            "-v",
        ]
        for k, v in config_dict.items():
            tilevcf_args.append("--tiledb-config")
            tilevcf_args.append("{}={}".format(k, v))

        response = job_info.client.submit_job(
            jobName="export_samples",
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

    print("Submitted export job(s) {}".format(" ".join(job_ids)))
    return job_ids


@click.command()
@click.option(
    "--dataset-uri",
    required=True,
    help="S3 URI of TileDB-VCF dataset to read from.",
    metavar="URI",
)
@click.option(
    "--dest-s3",
    required=True,
    help="S3 URI where exported BCFs will be stored.",
    metavar="URI",
)
@click.option(
    "--samples",
    required=True,
    help="S3 URI of file containing a list of sample names (one per"
    " line) to export. Must be on S3.",
    metavar="URI",
)
@click.option(
    "--bed",
    required=True,
    help="S3 URI of BED file containing sample regions to export.",
    metavar="URI",
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
    "--num-jobs",
    default=1,
    help="Number of jobs to submit for export. Each job will be "
    "responsible for exporting nsamples/njobs samples from the "
    "array.",
    metavar="N",
)
@click.option("--region", help="AWS region name", default="us-east-1", metavar="NAME")
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
    "--tiledb-config",
    metavar="key1=val1,key2=val2...",
    default=None,
    help="Extra TileDB config parameters to specify to tilevcf.",
)
def main(
    dataset_uri,
    samples,
    job_queue,
    job_definition,
    region,
    num_jobs,
    bed,
    dest_s3,
    retries,
    wait,
    tiledb_config,
):
    """Exports BCF sample data from a TileDB array via AWS Batch."""

    job_info = JobInfo(
        job_queue=job_queue,
        job_definition=job_definition,
        client=boto3.client("batch", region_name=region),
        max_retries=retries,
        num_export_jobs=num_jobs,
    )

    # Parse config parameters
    config_dict = {}
    if tiledb_config is not None:
        config_opts = tiledb_config.split(",")
        for kv in config_opts:
            key, val = kv.split("=")
            if len(key) == 0 or len(val) == 0:
                print('Error: invalid config param format "{}"'.format(kv))
                sys.exit(1)
            config_dict[key] = val

    job_ids = export_samples(dataset_uri, samples, bed, dest_s3, config_dict, job_info)

    if wait:
        stats = batchmetrics.wait_all(job_ids, job_info.client)
        batchmetrics.print_stats_report(stats, instance_price_per_hr=0.1596)
        if stats["num_succeeded"] != stats["num_jobs"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
