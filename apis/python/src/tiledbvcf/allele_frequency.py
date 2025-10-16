import pandas


def read_allele_frequency(dataset_uri: str, region: str) -> pandas.DataFrame:
    """
    Read variant status

    :param dataset_uri: dataset URI
    :param region: genomics region to read
    """
    import tiledbvcf

    ds = tiledbvcf.Dataset(uri=dataset_uri, mode="r")
    return ds.read_variant_stats(region)
