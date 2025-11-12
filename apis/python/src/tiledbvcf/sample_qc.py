import tiledbvcf.libtiledbvcf as clib


def sample_qc(
    dataset_uri,
    *,
    samples=[],
    config={},
):
    """
    Compute Sample QC metrics for a TileDB-VCF dataset.

    Parameters
    ----------
    dataset_uri : str
        URI of the TileDB-VCF dataset.

    samples : list of str, optional
        List of sample names to include in the QC metrics. If None, all samples are included.

    config : dict, optional
        TileDB configuration dictionary.

    Returns
    -------
    pandas.DataFrame
    """

    return clib.sample_qc(dataset_uri, samples=samples, config=config).to_pandas(
        split_blocks=True, self_destruct=True
    )
