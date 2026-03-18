# Copyright (c) TileDB, Inc.
# Licensed under the MIT License.

import pandas as pd


def read_allele_frequency(dataset_uri: str, region: str) -> pd.DataFrame:
    """
    Read variant status.

    :param dataset_uri: dataset URI
    :param region: genomics region to read
    """
    import tiledbvcf

    ds = tiledbvcf.Dataset(uri=dataset_uri, mode="r")
    return ds.read_variant_stats(region)
