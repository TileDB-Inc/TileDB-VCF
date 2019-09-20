#!/usr/bin/env python

import click
import random
import subprocess


def get_samples(array, num_samples, tilevcf):
    """Returns a list of random sample names in the given array."""
    tilevcf = 'tilevcf' if tilevcf is None else tilevcf
    cmd = [tilevcf, 'list', '-u', array]
    try:
        samples_str = subprocess.check_output(cmd)
    except Exception as e:
        print('Could not list samples in array: {}'.format(str(e)))
        return ''
    all_samples = samples_str.decode('utf-8').split('\n')
    return random.sample(all_samples, num_samples)


@click.command()
@click.option('--array', required=True,
              help='URI of TileDB array to select samples from.', metavar='URI')
@click.option('--num-samples', required=True,
              help='Number of random sample names to print.', metavar='N')
@click.option('--seed', help='If given, use as the seed for PRNG.', metavar='N',
              default=None)
@click.option('--tilevcf', help='Path to tilevcf executable (if not in PATH).',
              default=None, metavar='PATH')
def main(array, num_samples, seed, tilevcf):
    """Prints a subselection of random sample names from a TileDB VCF array."""
    if seed is not None:
        random.seed(int(seed))
    samples = get_samples(array, int(num_samples), tilevcf)
    for s in samples:
        print(s)


if __name__ == '__main__':
    main()
