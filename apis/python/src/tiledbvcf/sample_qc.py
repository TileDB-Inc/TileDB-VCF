import tiledb
import numpy as np


def sample_qc(
    dataset_uri,
    *,
    samples=None,
    config=None,
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

    if isinstance(samples, str):
        samples = [samples]
    if samples is None:
        samples = slice(None)

    with tiledb.scope_ctx(config):
        ss_uri = tiledb.Group(dataset_uri)["sample_stats"].uri

        with tiledb.open(ss_uri) as A:
            df = A.df[samples]

    agg_funcs = {col: "sum" for col in df.select_dtypes(include=[np.number]).columns}
    agg_funcs["dp_min"] = "min"
    agg_funcs["gq_min"] = "min"
    agg_funcs["dp_max"] = "max"
    agg_funcs["gq_max"] = "max"

    df = df.groupby("sample").agg(agg_funcs)

    def stddev(sum2, mean, count):
        return np.sqrt((sum2 - count * mean**2) / (count - 1))

    df["dp_mean"] = df.dp_sum / df.dp_count
    df["dp_stddev"] = stddev(df.dp_sum2, df.dp_mean, df.dp_count)
    df["gq_mean"] = df.gq_sum / df.gq_count
    df["gq_stddev"] = stddev(df.gq_sum2, df.gq_mean, df.gq_count)

    drop_cols = ["dp_sum", "dp_sum2", "dp_count", "gq_sum", "gq_sum2", "gq_count"]
    df = df.drop(drop_cols, axis=1)

    df["n_hom_var"] = df.n_called - df.n_hom_ref - df.n_het
    df["n_non_ref"] = df.n_called - df.n_hom_ref
    df["r_ti_tv"] = df.n_transition / df.n_transversion
    df["r_insertion_deletion"] = df.n_insertion / df.n_deletion
    df["r_het_hom_var"] = df.n_het / df.n_hom_var

    df["call_rate"] = df.n_called / (df.n_called + df.n_not_called)

    column_order = [
        "dp_mean",
        "dp_stddev",
        "dp_min",
        "dp_max",
        "gq_mean",
        "gq_stddev",
        "gq_min",
        "gq_max",
        "call_rate",
        "n_called",
        "n_not_called",
        "n_hom_ref",
        "n_het",
        "n_hom_var",
        "n_non_ref",
        "n_singleton",
        "n_snp",
        "n_insertion",
        "n_deletion",
        "n_transition",
        "n_transversion",
        "n_star",
        "r_ti_tv",
        "r_het_hom_var",
        "r_insertion_deletion",
        "n_records",
        "n_multi",
    ]

    return df[column_order].reset_index()
