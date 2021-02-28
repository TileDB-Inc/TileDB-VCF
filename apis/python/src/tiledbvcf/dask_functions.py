import dask
import dask.dataframe

import pyarrow as pa

from .dataset import ReadConfig, Dataset


class ReadArgs(object):
    def __init__(
        self,
        uri,
        cfg,
        attrs,
        region_partitions,
        sample_partitions,
        samples,
        regions,
        samples_file,
        bed_file,
        fnc,
    ):
        self.uri = uri
        self.cfg = cfg
        self.attrs = attrs
        self.region_partitions = region_partitions
        self.sample_partitions = sample_partitions
        self.samples = samples
        self.regions = regions
        self.samples_file = samples_file
        self.bed_file = bed_file
        self.fnc = fnc


def _read_partition(read_args):
    table_fragments = []

    ds = Dataset(read_args.uri, "r", read_args.cfg)
    pyar = ds.read_arrow(
        attrs=read_args.attrs,
        samples=read_args.samples,
        regions=read_args.regions,
        samples_file=read_args.samples_file,
        bed_file=read_args.bed_file,
    )
    table_fragments.append(pyar)

    while not ds.read_completed():
        table_fragments.append(ds.continue_read_arrow(release_buffers=True))

    table = pa.concat_tables(table_fragments, promote=False)

    df = table.to_pandas()

    if read_args.fnc is not None:
        df = read_args.fnc(df)

    return df


def map_dask(
    self,
    fnc,
    attrs,
    region_partitions=1,
    sample_partitions=1,
    limit_partitions=None,
    samples=None,
    regions=None,
    samples_file=None,
    bed_file=None,
):
    """Maps a function on a Dask dataframe obtained by reading from the dataset.

    May be more efficient in some cases than read_dask() followed by a regular
    Dask map operation.

    The remaining parameters are the same as the read_dask() function.

    :return: Dask DataFrame with results
    """
    cfg = self.cfg if self.cfg is not None else ReadConfig()
    partitions = []
    for r in range(0, region_partitions):
        for s in range(0, sample_partitions):
            r_part = (r, region_partitions)
            s_part = (s, sample_partitions)
            cfg = cfg._replace(region_partition=r_part, sample_partition=s_part)
            read_args = ReadArgs(
                self.uri,
                cfg,
                attrs,
                region_partitions,
                sample_partitions,
                samples,
                regions,
                samples_file,
                bed_file,
                fnc,
            )
            partitions.append(dask.delayed(_read_partition)(read_args))

            if limit_partitions is not None and len(partitions) >= limit_partitions:
                break
        else:
            continue
        break

    return dask.dataframe.from_delayed(partitions)


def read_dask(
    self,
    attrs,
    region_partitions=1,
    sample_partitions=1,
    limit_partitions=None,
    samples=None,
    regions=None,
    samples_file=None,
    bed_file=None,
):
    """Reads data from a TileDB-VCF into a Dask DataFrame.

    Partitioning proceeds by a straightforward block distribution, parameterized
    by the total number of partitions and the particular partition index that
    a particular read operation is responsible for.

    Both region and sample partitioning can be used together.

    The remaining parameters are the same as the normal read() function.

    :param int region_partition: Number of partitions over regions
    :param int sample_partition: Number of partitions over samples
    :param int limit_partitions: Maximum number of partitions to read (for testing/debugging)

    :return: Dask DataFrame with results
    """

    return map_dask(
        self,
        None,
        attrs,
        region_partitions,
        sample_partitions,
        limit_partitions,
        samples,
        regions,
        samples_file,
        bed_file,
    )


# Patch functions and members into dataset class
Dataset.read_dask = read_dask
Dataset.map_dask = map_dask
