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

        self.reader.read()

        results = self.reader.get_results()
        df_series = {}
        for attr_name, buffs in results.items():
            offsets, data = buffs[0], buffs[1]
            has_offsets = offsets.size > 0
            if has_offsets:
                values = []
                for i, offset in enumerate(offsets):
                    if i < len(offsets) - 1:
                        next_offset = offsets[i + 1]
                    else:
                        next_offset = len(data)
                    value = data[offset:next_offset]
                    if data.dtype.char == 'S':
                        values.append(''.join(value.astype(str)))
                    else:
                        values.append(value)
                df_series[attr_name] = pd.Series(values)
            else:
                df_series[attr_name] = pd.Series(data)
        return pd.DataFrame.from_dict(df_series)

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
