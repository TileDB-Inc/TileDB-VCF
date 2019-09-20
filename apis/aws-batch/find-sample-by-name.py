#!/usr/bin/env python

import boto3
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


def find_sample(bucket, dir, sample, tmpdir_path, min_val, max_val):
    """Print URI of a sample given the name."""
    s3 = boto3.resource('s3')
    client = s3.meta.client
    range_str = 'bytes={}-{}'.format(0, 10 * 1024 * 1024)
    with tempfile.TemporaryDirectory(dir=tmpdir_path) as tmpdir:
        for i in range(min_val, max_val):
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
                csi_tmpfile = os.path.join(tmpdir, '{}.part.bcf.csi'.format(i))
                with open(csi_tmpfile, 'wb') as f:
                    f.write(resp['Body'].read())

                # Check if the sample should be included.
                if get_sample_name(bcf_tmpfile) == sample:
                    final_uri = 's3://{}/{}'.format(bucket, bcf_uri)
                    print(final_uri)
                    return

                # Clean up
                os.remove(bcf_tmpfile)
                os.remove(csi_tmpfile)
            except:
                # Ignore exceptions; try next sample.
                pass
    print('Sample \'{}\' not found.'.format(sample))


@click.command()
@click.option('--bucket', required=True, help='S3 bucket URI.', metavar='URI')
@click.option('--dir', required=True,
              help='Path of directory (relative to S3 bucket) containing BCF '
                   'files to search.',
              metavar='URI')
@click.option('--sample', required=True,
              help='Name of sample to find.', metavar='name')
@click.option('--min', default=1,
              help='Minimum bound of filenames to search, e.g. N.bcf',
              metavar='N')
@click.option('--max', default=50000,
              help='Maximum bound of filenames to search, e.g. M.bcf',
              metavar='M')
@click.option('--tmpdir', default=None,
              help='Path of temporary directory to use for scratch space')
def main(bucket, dir, sample, tmpdir, min, max):
    """Finds URI of sample given the name and a list of BCFs to search.."""
    if bucket.startswith('s3://'):
        bucket = bucket.replace('s3://', '')
    if dir.endswith('/'):
        dir = dir[:-1]
    find_sample(bucket, dir, sample, tmpdir, int(min), int(max))


if __name__ == '__main__':
    main()
