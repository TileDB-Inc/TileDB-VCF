import pandas as pd
from . import libtiledbvcf


class TileDBVCFDataset(object):
    """A handle on a TileDB-VCF dataset."""

    def __init__(self, uri, mode='r'):
        """ Initializes a TileDB-VCF dataset for interaction.

        :param uri: URI of TileDB-VCF dataset
        :param mode: Mode of operation.
        :type mode: 'r' or 'w'
        """
        if mode == 'r':
            self.reader = libtiledbvcf.Reader()
            self.reader.init(uri)
        elif mode == 'w':
            self.writer = libtiledbvcf.Writer()
            self.writer.init(uri)
        else:
            raise Exception('Unsupported dataset mode {}'.format(mode))

    def read(self, attrs, samples=None, regions=None):
        """Reads data from a TileDB-VCF dataset.

        For large datasets, a call to `read()` may not be able to fit all
        results in memory. In that case, the returned dataframe will contain as
        many results as possible, and in order to retrieve the rest of the
        results, use the `continue_read()` function.

        You can also use the Python generator version, `read_iter()`.

        :param list of str attrs: List of attribute names to be read.
        :param list of str samples: CSV list of sample names to be read.
        :param list of str regions: CSV list of genomic regions to be read.
        :return: Pandas DataFrame containing results.
        """
        self.reader.reset()

        samples = '' if samples is None else samples
        regions = '' if regions is None else regions
        self.reader.set_samples(','.join(samples))
        self.reader.set_regions(','.join(regions))
        self.reader.set_attributes(attrs)

        return self.continue_read()

    def read_iter(self, attrs, samples=None, regions=None):
        if not self.read_completed():
            yield self.read(attrs, samples, regions)
        while not self.read_completed():
            yield self.continue_read()

    def continue_read(self):
        self.reader.read()
        table = self.reader.get_results_arrow()
        return table.to_pandas()

    def read_completed(self):
        """Returns true if the previous read operation was complete.

        A read is considered complete if the resulting dataframe contained
        all results."""
        return self.reader.completed()

    def count(self, samples=None, regions=None):
        """Counts data in a TileDB-VCF dataset.

        :param list of str samples: CSV list of sample names to include in
            the count.
        :param list of str regions: CSV list of genomic regions include in
            the count
        :return: Number of intersecting records in the dataset
        """
        self.reader.reset()

        samples = '' if samples is None else samples
        regions = '' if regions is None else regions
        self.reader.set_samples(','.join(samples))
        self.reader.set_regions(','.join(regions))

        self.reader.read()
        if not self.read_completed():
            raise Exception('Unexpected read status during count.')

        return self.reader.result_num_records()

    def ingest_samples(self, sample_uris=None, extra_attrs=None):
        if sample_uris is None:
            return

        self.writer.set_samples(','.join(sample_uris))

        extra_attrs = '' if extra_attrs is None else extra_attrs
        self.writer.set_extra_attributes(','.join(extra_attrs))

        # Create is a no-op if the dataset already exists.
        self.writer.create_dataset()
        self.writer.register_samples()
        self.writer.ingest_samples()
