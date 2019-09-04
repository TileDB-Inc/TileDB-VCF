#!/usr/bin/env python

import boto3
import botocore
import click
import subprocess
import sys
import tempfile
import os


def get_sample_name(path):
    """Get the name of the sample in the given BCF file."""
    try:
        cmd = 'bcftools query -l {}'.format(path).split()
        output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode(
            'utf-8').split('\n')
        return output[0]
    except:
        return None


def get_last_contig_name(path):
    """Get the name of the last contig in the given BCF file."""
    try:
        cmd = 'bcftools index -s {}'.format(path).split()
        output = subprocess.check_output(cmd).decode('utf-8').split('\n')
        last = output[-2].split()
        return last[0]
    except:
        return None


def get_samples(bucket, dir, num_samples, last_contig, tmpdir_path,
                sample_name_blacklist, update_blacklist):
    """Print a list of sample URIs that all have the given last contig."""
    s3 = boto3.resource('s3')
    client = s3.meta.client
    range_str = 'bytes={}-{}'.format(0, 10 * 1024 * 1024)
    result_num_samples = 0
    max_sample_name = 45000

    # Prepopulate the sample names blacklist
    samples_seen = set()
    if sample_name_blacklist is not None and os.path.exists(
            sample_name_blacklist):
        with open(sample_name_blacklist, 'r') as f:
            for line in f:
                samples_seen |= {line.strip()}

    # Get the list of samples, printing to stdout.
    with tempfile.TemporaryDirectory(dir=tmpdir_path) as tmpdir:
        i = 0
        while result_num_samples < num_samples and i < max_sample_name:
            i += 1
            try:
                # Download some bytes of BCF
                bcf_uri = '{}/{}.bcf'.format(dir, i)
                resp = client.get_object(Bucket=bucket, Key=bcf_uri,
                                         Range=range_str)
                bcf_tmpfile = os.path.join(tmpdir, '{}.part.bcf'.format(i))
                with open(bcf_tmpfile, 'wb') as f:
                    f.write(resp['Body'].read())

                # Download CSI index file
                csi_uri = '{}/{}.bcf.csi'.format(dir, i)
                resp = client.get_object(Bucket=bucket, Key=csi_uri)
                csi_tmpfile = os.path.join(tmpdir,
                                           '{}.part.bcf.csi'.format(i))
                with open(csi_tmpfile, 'wb') as f:
                    f.write(resp['Body'].read())

                # Check if the sample should be included.
                sample_name = get_sample_name(bcf_tmpfile)
                if (get_last_contig_name(bcf_tmpfile) == last_contig and
                        sample_name not in samples_seen):
                    final_uri = 's3://{}/{}'.format(bucket, bcf_uri)
                    sys.stdout.write('{}\n'.format(final_uri))
                    sys.stdout.flush()
                    samples_seen |= {sample_name}
                    result_num_samples += 1

                # Clean up
                os.remove(bcf_tmpfile)
                os.remove(csi_tmpfile)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    pass
                else:
                    raise

    # Update the blacklist, if specified.
    if update_blacklist:
        with open(sample_name_blacklist, 'w') as f:
            for s in samples_seen:
                f.write('{}\n'.format(s))


@click.command()
@click.option('--bucket', required=True, help='S3 bucket URI.', metavar='URI')
@click.option('--dir', required=True,
              help='Path of directory (relative to S3 bucket) containing BCF '
                   'files.',
              metavar='URI')
@click.option('--num-samples', required=True,
              help='Number of samples to get.', metavar='N')
@click.option('--tmpdir', default=None,
              help='Path of temporary directory to use for scratch space')
@click.option('--last-contig', default='chrY',
              help='Last contig name (in order given by \'bcftools index -s\') '
                   'that a sample must have to be added to the list.',
              metavar='NAME')
@click.option('--sample-name-blacklist', default=None,
              help='Path to file containing a list of sample names to exclude, '
                   'one per line.', metavar='PATH')
@click.option('--update-blacklist',
              help='If specified, update the blacklist file with resulting '
                   'sample names.', is_flag=True)
def main(bucket, dir, num_samples, last_contig, tmpdir, sample_name_blacklist,
         update_blacklist):
    """Compiles a list of sample URIs from a directory of BCFs on S3.

    Examples:

    Get the list of the first 10 samples (numerically) from S3 directory
    s3://my-bucket/bcfs/:

        get-sample-list.py --bucket my-bucket --dir bcfs --num-samples 10

    Same, but append the resulting sample names to a blacklist file:

        get-sample-list.py --bucket my-bucket --dir bcfs --num-samples 10 \
            --sample-name-blacklist blacklist.txt --update-blacklist

    Same, but use an existing blacklist file to filter out samples by name
    (not updating the blacklist file):

        get-sample-list.py --bucket my-bucket --dir bcfs --num-samples 10 \
            --sample-name-blacklist blacklist.txt
    """
    if bucket.startswith('s3://'):
        bucket = bucket.replace('s3://', '')
    if dir.endswith('/'):
        dir = dir[:-1]

    get_samples(bucket, dir, int(num_samples), last_contig, tmpdir,
                sample_name_blacklist, update_blacklist)


if __name__ == '__main__':
    main()
