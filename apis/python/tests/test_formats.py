import os
import platform
import shutil

import numpy as np
import pandas as pd
import pytest
import tiledb
import tiledbvcf

from .conftest import assert_dfs_equal, skip_if_no_bcftools, TESTS_INPUT_DIR

@skip_if_no_bcftools
def test_gvcf_export(tmp_path, bgzip_and_index_vcfs):
    vcf_files = bgzip_and_index_vcfs(
        os.path.join(TESTS_INPUT_DIR, "gvcf-export"), output_dir=str(tmp_path)
    )

    # Ingest the VCFs
    uri = os.path.join(tmp_path, "vcf.tdb")
    ds = tiledbvcf.Dataset(uri=uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples(vcf_files)
    ds = tiledbvcf.Dataset(uri=uri, mode="r")

    # List of tests.
    tests = [
        {"region": "chr1:100-120", "samples": ["s0", "s1", "s2"]},
        {"region": "chr1:110-120", "samples": ["s0", "s1"]},
        {"region": "chr1:149-149", "samples": ["s0", "s1", "s3"]},
        {"region": "chr1:150-150", "samples": ["s0", "s1", "s3", "s4"]},
    ]

    # No IAF filtering or reporting
    for test in tests:
        df = ds.read(regions=test["region"])
        assert set(df["sample_name"].unique()) == set(test["samples"])

    attrs = [
        "sample_name",
        "contig",
        "pos_start",
        "alleles",
        "fmt_GT",
        "info_TILEDB_IAF",
    ]

    # IAF reporting
    for test in tests:
        df = ds.read(attrs=attrs, regions=test["region"])
        assert set(df["sample_name"].unique()) == set(test["samples"])

    # IAF filtering and reporting
    for test in tests:
        df = ds.read(attrs=attrs, regions=test["region"], set_af_filter="<=1.0")
        assert set(df["sample_name"].unique()) == set(test["samples"])


def test_flag_export(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.vcf.gz"]]
    ds.create_dataset()
    ds.ingest_samples(samples)

    # Read info flags
    ds = tiledbvcf.Dataset(uri, mode="r")
    df = ds.read(attrs=["pos_start", "info_DB", "info_DS"])
    df = df.sort_values(by=["pos_start"])

    # Check if flags match the expected values
    expected_db = [1, 1, 1, 0, 0, 1]
    assert df["info_DB"].tolist() == expected_db

    expected_ds = [1, 1, 0, 0, 1, 1]
    assert df["info_DS"].tolist() == expected_ds


@pytest.mark.parametrize("use_arrow", [False, True], ids=["pandas", "arrow"])
def test_bed_filestore(tmp_path, v4_dataset, use_arrow):
    # tiledbvcf.config_logging("debug")

    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                [
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                ]
            ),
            "pos_start": pd.Series(
                [
                    12141,
                    12141,
                    12546,
                    12546,
                    17319,
                ],
                dtype=np.int32,
            ),
            "pos_end": pd.Series(
                [
                    12277,
                    12277,
                    12771,
                    12771,
                    17479,
                ],
                dtype=np.int32,
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])

    # Create BED file
    bed_file = os.path.join(tmp_path, "test.bed")

    regions = [
        (1, 12000, 13000),
        (1, 17000, 17479),
    ]

    with open(bed_file, "w") as f:
        for region in regions:
            f.write(f"{region[0]}\t{region[1]}\t{region[2]}\n")

    # Create BED filestore from BED file
    bed_filestore = os.path.join(tmp_path, "test.bed.filestore")
    tiledb.Array.create(bed_filestore, tiledb.ArraySchema.from_file(bed_file))
    tiledb.Filestore.copy_from(bed_filestore, bed_file)

    func = v4_dataset.read_arrow if use_arrow else v4_dataset.read
    df = func(attrs=["sample_name", "pos_start", "pos_end"], bed_file=bed_filestore)
    if use_arrow:
        df = df.to_pandas()
    assert_dfs_equal(
        expected_df,
        df.sort_values(ignore_index=True, by=["sample_name", "pos_start"]),
    )


@pytest.mark.parametrize("use_arrow", [False, True], ids=["pandas", "arrow"])
def test_bed_array(tmp_path, v4_dataset, use_arrow):
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                [
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                ]
            ),
            "pos_start": pd.Series(
                [
                    12141,
                    12141,
                    12546,
                    12546,
                    17319,
                ],
                dtype=np.int32,
            ),
            "pos_end": pd.Series(
                [
                    12277,
                    12277,
                    12771,
                    12771,
                    17479,
                ],
                dtype=np.int32,
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])

    # Create bed array
    bed_array = os.path.join(tmp_path, "bed_array")
    tiledb.from_pandas(
        bed_array,
        pd.DataFrame(
            {
                "chrom": ["1", "1"],
                "chromStart": [12000, 17000],
                "chromEnd": [13000, 17479],
            }
        ),
        sparse=True,
        index_col=["chrom", "chromStart"],
    )

    # Add aliases to the array metadata
    with tiledb.Array(bed_array, "w") as A:
        A.meta["alias contig"] = "chrom"
        A.meta["alias start"] = "chromStart"
        A.meta["alias end"] = "chromEnd"

    func = v4_dataset.read_arrow if use_arrow else v4_dataset.read
    df = func(attrs=["sample_name", "pos_start", "pos_end"], bed_file=bed_array)
    if use_arrow:
        df = df.to_pandas()

        assert_dfs_equal(
            expected_df,
            df.sort_values(ignore_index=True, by=["sample_name", "pos_start"]),
        )

def test_info_end(tmp_path):
    """
    This test checks that the info_END attribute is handled correctly, even when the
    VCF header incorrectly defines the END attribute as a string.

    The test also checks that info_END contains the original values from the VCF,
    including the missing values.
    """

    expected_end = pd.DataFrame(
        {
            "pos_end": pd.Series(
                [
                    12277,
                    12771,
                    13374,
                    13395,
                    13413,
                    13451,
                    13519,
                    13544,
                    13689,
                    17479,
                    17486,
                    30553,
                    35224,
                    35531,
                    35786,
                    69096,
                    69103,
                    69104,
                    69109,
                    69110,
                    69111,
                    69112,
                    69114,
                    69115,
                    69122,
                    69123,
                    69128,
                    69129,
                    69130,
                    69192,
                    69195,
                    69196,
                    69215,
                    69222,
                    69227,
                    69228,
                    69261,
                    69262,
                    69269,
                    69270,
                    69346,
                    69349,
                    69352,
                    69353,
                    69370,
                    69510,
                    69511,
                    69760,
                    69761,
                    69770,
                    69834,
                    69835,
                    69838,
                    69861,
                    69863,
                    69866,
                    69896,
                    69897,
                    69912,
                    69938,
                    69939,
                    69941,
                    69946,
                    69947,
                    69948,
                    69949,
                    69953,
                    70012,
                    866511,
                    1289369,
                ],
                dtype=np.int32,
            ),
            # Expected values are strings because the small3.vcf.gz defines END as a string
            "info_END": pd.Series(
                [
                    "12277",
                    "12771",
                    "13374",
                    "13395",
                    "13413",
                    "13451",
                    "13519",
                    "13544",
                    "13689",
                    "17479",
                    "17486",
                    "30553",
                    "35224",
                    "35531",
                    "35786",
                    "69096",
                    "69103",
                    "69104",
                    "69109",
                    "69110",
                    "69111",
                    "69112",
                    "69114",
                    "69115",
                    "69122",
                    "69123",
                    "69128",
                    "69129",
                    "69130",
                    "69192",
                    "69195",
                    "69196",
                    "69215",
                    "69222",
                    "69227",
                    "69228",
                    "69261",
                    "69262",
                    "69269",
                    None,
                    "69346",
                    "69349",
                    "69352",
                    "69353",
                    "69370",
                    "69510",
                    None,
                    "69760",
                    None,
                    "69770",
                    "69834",
                    "69835",
                    "69838",
                    "69861",
                    "69863",
                    "69866",
                    "69896",
                    None,
                    "69912",
                    "69938",
                    "69939",
                    "69941",
                    "69946",
                    "69947",
                    "69948",
                    "69949",
                    "69953",
                    "70012",
                    None,
                    None,
                ],
                dtype=object,
            ),
        }
    )

    # Ingest the data
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small3.vcf.gz"]]
    ds.create_dataset()
    ds.ingest_samples(samples)

    # Read the data
    ds = tiledbvcf.Dataset(uri)
    df = ds.read(attrs=["sample_name", "pos_start", "pos_end", "info_END"])

    # Sort the results because VCF uses an unordered reader
    df.sort_values(ignore_index=True, by=["sample_name", "pos_start"], inplace=True)

    # Drop the columns that are not used for comparison
    df.drop(columns=["sample_name", "pos_start"], inplace=True)

    # Check the results
    assert_dfs_equal(df, expected_end)

def test_equality_old_new_format():
    old_ds = tiledbvcf.Dataset(os.path.join(TESTS_INPUT_DIR, "arrays/old_format"))
    new_ds = tiledbvcf.Dataset(os.path.join(TESTS_INPUT_DIR, "arrays/new_format"))

    assert old_ds.count() == new_ds.count()
    assert old_ds.samples() == new_ds.samples()
    assert old_ds.read().equals(new_ds.read())
