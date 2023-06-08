import pandas


def _calc_af(df) -> pandas.DataFrame:
    """
    Consolidate AC and compute AN, AF

    :param pandas.Dataframe df
    """
    # Allele Count (AC) = sum of all AC at the same locus
    # This step consolidates ACs from all ingested batches
    df = df.groupby(["pos", "allele"], sort=True).sum(numeric_only=True)

    # Allele Number (AN) = sum of AC at the same locus
    an = df.groupby(["pos"], sort=True).ac.sum().rename("an")
    df = df.join(an, how="inner").reset_index()

    # Allele Frequency (AF) = AC / AN
    df["af"] = df.ac / df.an
    return df


def read_allele_frequency(dataset_uri: str, region: str) -> pandas.DataFrame():
    """
    Read variant status

    :param dataset_uri: dataset URI
    :param region: genomics region to read
    """
    import tiledb

    # Get the variant stats uri
    with tiledb.Group(dataset_uri) as g:
        alleles_uri = g["variant_stats"].uri

        try:
            contig = region.split(":")[0]
            start, end = map(int, region.split(":")[1].split("-"))
            region_slice = slice(start, end)
        except Exception as e:
            raise ValueError(
                f"Invalid region: {region}. Expected format: contig:start-end"
            ) from e

        with tiledb.open(alleles_uri) as A:
            df = A.query(attrs=["ac", "allele"], dims=["pos", "contig"]).df[
                contig, region_slice
            ]

        return _calc_af(df)
