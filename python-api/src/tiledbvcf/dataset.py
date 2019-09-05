import pandas as pd
from . import libtiledbvcf


class TileDBVCFDataset(object):
    """A handle on a TileDB-VCF dataset."""

    def __init__(self, uri, mode='r'):
        """ Initializes a TileDB-VCF dataset for interaction.

        :param uri: URI of TileDB-VCF dataset
        :param mode: Mode of operation.
        :type mode: 'r' or 'w' (currently unsupported)
        """
        if mode != 'r':
            raise Exception('Unsupported dataset mode {}'.format(mode))
        self.reader = libtiledbvcf.Reader()
        self.reader.init(uri)

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
