import pandas as pd
import sys
import warnings

from collections import namedtuple
from . import libtiledbvcf

ReadConfig = namedtuple('ReadConfig', [
    # Max number of records (rows) to read.
    'limit',
    # Region partition tuple (idx, num_partitions)
    'region_partition',
    # Samples partition tuple (idx, num_partitions)
    'sample_partition',
    # Whether or not to sort the regions to be read (default True)
    'sort_regions',
    # Memory budget (MB) for buffer and internal allocations (default 2048MB)
    'memory_budget_mb',
    # List of strings of format 'option=value'
    'tiledb_config'
])
ReadConfig.__new__.__defaults__ = (None,) * 6#len(ReadConfig._fields)


class Dataset(object):
    """A handle on a TileDB-VCF dataset."""

    def __init__(self, uri, mode='r', cfg=None, stats=False, verbose=False):
        """ Initializes a TileDB-VCF dataset for interaction.

        :param uri: URI of TileDB-VCF dataset
        :param mode: Mode of operation.
        :type mode: 'r' or 'w'
        :param cfg: TileDB VCF configuration (optional)
        :param stats: Enable or disable TileDB stats (optional)
        :param verbose: Enable or disable TileDB VCF verbose output (optional)
        """
        self.uri = uri
        self.mode = mode
        self.cfg = cfg
        if self.mode == 'r':
            self.reader = libtiledbvcf.Reader()
            self._set_read_cfg(cfg)
            self.reader.init(uri)
            self.reader.set_tiledb_stats_enabled(stats)
            self.reader.set_verbose(verbose)
        elif self.mode == 'w':
            self.writer = libtiledbvcf.Writer()
            self.writer.init(uri)
            self.writer.set_verbose(verbose)
            if cfg is not None:
                raise Exception('Config not supported in write mode')
        else:
            raise Exception('Unsupported dataset mode {}'.format(mode))

    def _set_read_cfg(self, cfg):
        if cfg is None:
            return
        if cfg.limit is not None:
            self.reader.set_max_num_records(cfg.limit)
        if cfg.region_partition is not None:
            self.reader.set_region_partition(*cfg.region_partition)
        if cfg.sample_partition is not None:
            self.reader.set_sample_partition(*cfg.sample_partition)
        if cfg.sort_regions is not None:
            self.reader.set_sort_regions(cfg.sort_regions)
        if cfg.memory_budget_mb is not None:
            self.reader.set_memory_budget(cfg.memory_budget_mb)
        if cfg.tiledb_config is not None:
            tiledb_config_list = list()
            if  isinstance(cfg.tiledb_config, list):
                tiledb_config_list = cfg.tiledb_config
            # Support dictionaries and tiledb.Config objects also
            elif isinstance(cfg.tiledb_config, dict):
                for key in cfg.tiledb_config:
                    if cfg.tiledb_config[key] != "":
                        tiledb_config_list.append("{}={}".format(key, cfg.tiledb_config[key]))
            else:
                try:
                    import tiledb
                    if isinstance(cfg.tiledb_config, tiledb.Config):
                        for key in cfg.tiledb_config:
                            if cfg.tiledb_config[key] != "":
                                tiledb_config_list.append("{}={}".format(key, cfg.tiledb_config[key]))
                except ImportError:
                    pass
            self.reader.set_tiledb_config(','.join(tiledb_config_list))

    def read(self, attrs, samples=None, regions=None, samples_file=None,
             bed_file=None):
        """Reads data from a TileDB-VCF dataset.

        For large datasets, a call to `read()` may not be able to fit all
        results in memory. In that case, the returned dataframe will contain as
        many results as possible, and in order to retrieve the rest of the
        results, use the `continue_read()` function.

        You can also use the Python generator version, `read_iter()`.

        :param list of str attrs: List of attribute names to be read.
        :param list of str samples: CSV list of sample names to be read.
        :param list of str regions: CSV list of genomic regions to be read.
        :param str samples_file: URI of file containing sample names to be read,
            one per line.
        :param str bed_file: URI of a BED file of genomic regions to be read.
        :return: Pandas DataFrame containing results.
        """
        if self.mode != 'r':
            raise Exception('Dataset not open in read mode')

        self.reader.reset()

        samples = '' if samples is None else samples
        regions = '' if regions is None else regions
        self.reader.set_samples(','.join(samples))
        self.reader.set_regions(','.join(regions))
        self.reader.set_attributes(attrs)

        if samples_file is not None:
            self.reader.set_samples_file(samples_file)

        if bed_file is not None:
            self.reader.set_bed_file(bed_file)

        return self.continue_read()

    def read_iter(self, attrs, samples=None, regions=None, samples_file=None,
                  bed_file=None):
        if self.mode != 'r':
            raise Exception('Dataset not open in read mode')

        if not self.read_completed():
            yield self.read(attrs, samples, regions, samples_file, bed_file)
        while not self.read_completed():
            yield self.continue_read()

    def continue_read(self):
        if self.mode != 'r':
            raise Exception('Dataset not open in read mode')

        self.reader.read()
        table = self.reader.get_results_arrow()
        return table.to_pandas()

    def read_completed(self):
        """Returns true if the previous read operation was complete.

        A read is considered complete if the resulting dataframe contained
        all results."""
        if self.mode != 'r':
            raise Exception('Dataset not open in read mode')
        return self.reader.completed()

    def count(self, samples=None, regions=None):
        """Counts data in a TileDB-VCF dataset.

        :param list of str samples: CSV list of sample names to include in
            the count.
        :param list of str regions: CSV list of genomic regions include in
            the count
        :return: Number of intersecting records in the dataset
        """
        if self.mode != 'r':
            raise Exception('Dataset not open in read mode')
        self.reader.reset()

        samples = '' if samples is None else samples
        regions = '' if regions is None else regions
        self.reader.set_samples(','.join(samples))
        self.reader.set_regions(','.join(regions))

        self.reader.read()
        if not self.read_completed():
            raise Exception('Unexpected read status during count.')

        return self.reader.result_num_records()

    def ingest_samples(self, sample_uris=None, extra_attrs=None, checksum_type=None, allow_duplicates=True, scratch_space_path=None, scratch_space_size=None):
        """Ingest samples

        :param list of str samples: CSV list of sample names to include in
            the count.
        :param list of str extra_attrs: CSV list of extra attributes to
            materialize from fmt field
        :param str checksum_type: Optional override checksum type for creating
            new dataset valid values are sha256, md5 or none.
        :param str scratch_space_path: Directory used for local storage of
            downloaded remote samples.
        :param int scratch_space_size: Amount of local storage that can be used
            for downloading remote samples (MB).
        """

        if self.mode != 'w':
            raise Exception('Dataset not open in write mode')

        if sample_uris is None:
            return

        if checksum_type is not None:
            checksum_type = checksum_type.lower()
            self.writer.set_checksum(checksum_type)

        self.writer.set_allow_duplicates(allow_duplicates)

        if scratch_space_path is not None and scratch_space_size is not None:
            self.writer.set_scratch_space(scratch_space_path, scratch_space_size)
        elif scratch_space_path is not None or scratch_space_size is not None:
            raise Exception('Must set both scratch_space_path and scratch_space_size to use scratch space')

        self.writer.set_samples(','.join(sample_uris))

        extra_attrs = '' if extra_attrs is None else extra_attrs
        self.writer.set_extra_attributes(','.join(extra_attrs))

        # Create is a no-op if the dataset already exists.
        self.writer.create_dataset()
        self.writer.register_samples()
        self.writer.ingest_samples()

    def tiledb_stats(self):
        if self.mode != 'r':
            raise Exception('Stats can only be called for reader')

        if not self.reader.get_tiledb_stats_enabled:
            raise Exception('Stats not enabled')

        return self.reader.get_tiledb_stats();

    def attributes(self):
        """List available attributes ingested from the VCF INFO and FORMAT fields

        :returns: a list of strings representing the attribute names
        """

        if self.mode != 'r':
            raise Exception("Attributes can only be retrieved in read mode")

        n_info = self.reader.get_info_attribute_count();
        n_fmt = self.reader.get_fmt_attribute_count();

        attrs = []
        for i in range(n_info):
            attrs.append(self.reader.get_info_attribute_name(i))
        for i in range(n_fmt):
            attrs.append(self.reader.get_fmt_attribute_name(i))

        return attrs

    def fmt_attr_count(self):
        if self.mode != 'r':
            raise Exception('Attributes can only be counted for in read mode')

        return self.reader.get_fmt_attribute_count();

    def fmt_attr_name(self, index):
        if self.mode != 'r':
            raise Exception('Attributes can only be retrieved in read mode')

        return self.reader.get_fmt_attribute_name(index);

    def info_attr_count(self):
        if self.mode != 'r':
            raise Exception('Attributes can only be counted for in read mode')

        return self.reader.get_info_attribute_count();

    def info_attr_name(self, index):
        if self.mode != 'r':
            raise Exception('Attributes can only be retrieved in read mode')

        return self.reader.get_info_attribute_name(index);

class TileDBVCFDataset(Dataset):
    """A handle on a TileDB-VCF dataset."""

    def __init__(self, uri, mode='r', cfg=None, stats=False, verbose=False):
        """ Initializes a TileDB-VCF dataset for interaction.

        :param uri: URI of TileDB-VCF dataset
        :param mode: Mode of operation.
        :type mode: 'r' or 'w'
        :param cfg: TileDB VCF configuration (optional)
        :param stats: Enable or disable TileDB stats (optional)
        :param verbose: Enable or disable TileDB VCF verbose output (optional)
        """
        warnings.warn(
            "TileDBVCFDataset is deprecated, use Dataset instead",
            DeprecationWarning
        )
        super().__init__(uri, mode, cfg, stats, verbose)
