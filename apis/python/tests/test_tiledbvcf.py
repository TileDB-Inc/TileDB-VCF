import glob
import os
import platform
import shutil
import subprocess

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import tiledb
import tiledbvcf

from .conftest import assert_dfs_equal, skip_if_incompatible, TESTS_INPUT_DIR


def test_basic_count(v3_dataset):
    assert v3_dataset.count() == 14


def test_retrieve_attributes(v3_dataset):
    builtin_attrs = [
        "sample_name",
        "contig",
        "pos_start",
        "pos_end",
        "alleles",
        "id",
        "fmt",
        "info",
        "filters",
        "qual",
        "query_bed_end",
        "query_bed_start",
        "query_bed_line",
    ]
    assert sorted(v3_dataset.attributes(attr_type="builtin")) == sorted(builtin_attrs)

    info_attrs = [
        "info_BaseQRankSum",
        "info_ClippingRankSum",
        "info_DP",
        "info_DS",
        "info_END",
        "info_HaplotypeScore",
        "info_InbreedingCoeff",
        "info_MLEAC",
        "info_MLEAF",
        "info_MQ",
        "info_MQ0",
        "info_MQRankSum",
        "info_ReadPosRankSum",
    ]
    assert v3_dataset.attributes(attr_type="info") == info_attrs

    fmt_attrs = [
        "fmt_AD",
        "fmt_DP",
        "fmt_GQ",
        "fmt_GT",
        "fmt_MIN_DP",
        "fmt_PL",
        "fmt_SB",
    ]
    assert v3_dataset.attributes(attr_type="fmt") == fmt_attrs


def test_retrieve_samples(v3_dataset):
    assert v3_dataset.samples() == ["HG00280", "HG01762"]


def test_read_unsupported_regions_type(v3_dataset):
    unsupported_region = 3.14
    unsupported_type_error = f'"regions" parameter cannot have type: {type(unsupported_region)}'
    wrong_dimension_region = np.array([["1:12700-13400"], ["1:12700-13400"]])
    ndarray_wrong_dimension_error = f'"regions" parameter of type {type(wrong_dimension_region)} must be 1-dimensional'
    with pytest.raises(Exception, match=unsupported_type_error):
        v3_dataset.read(regions=unsupported_region)
    with pytest.raises(Exception, match=ndarray_wrong_dimension_error):
        v3_dataset.read(regions=wrong_dimension_region)
    with pytest.raises(Exception, match=unsupported_type_error):
        v3_dataset.read_arrow(regions=unsupported_region)
    with pytest.raises(Exception, match=ndarray_wrong_dimension_error):
        v3_dataset.read_arrow(regions=wrong_dimension_region)
    with pytest.raises(Exception, match=unsupported_type_error):
        for variant in v3_dataset.read_iter(regions=unsupported_region):
            print(variant)
    with pytest.raises(Exception, match=ndarray_wrong_dimension_error):
        for variant in v3_dataset.read_iter(regions=wrong_dimension_region):
            print(variant)


def test_read_attrs(v3_dataset_with_attrs):
    attrs = ["sample_name"]
    df = v3_dataset_with_attrs.read(attrs=attrs)
    assert df.columns.values.tolist() == attrs

    attrs = ["sample_name", "fmt_GT"]
    df = v3_dataset_with_attrs.read(attrs=attrs)
    assert df.columns.values.tolist() == attrs

    attrs = ["sample_name"]
    df = v3_dataset_with_attrs.read(attrs=attrs)
    assert df.columns.values.tolist() == attrs


@pytest.mark.parametrize("use_arrow", [False, True], ids=["pandas", "arrow"])
def test_basic_reads(v3_dataset, use_arrow):
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                [
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                ]
            ),
            "pos_start": pd.Series(
                [
                    12141,
                    12141,
                    12546,
                    12546,
                    13354,
                    13354,
                    13375,
                    13396,
                    13414,
                    13452,
                    13520,
                    13545,
                    17319,
                    17480,
                ],
                dtype=np.int32,
            ),
            "pos_end": pd.Series(
                [
                    12277,
                    12277,
                    12771,
                    12771,
                    13374,
                    13389,
                    13395,
                    13413,
                    13451,
                    13519,
                    13544,
                    13689,
                    17479,
                    17486,
                ],
                dtype=np.int32,
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])

    func = v3_dataset.read_arrow if use_arrow else v3_dataset.read
    df = func(attrs=["sample_name", "pos_start", "pos_end"])
    if use_arrow:
        df = df.to_pandas()
    assert_dfs_equal(
        expected_df,
        df.sort_values(ignore_index=True, by=["sample_name", "pos_start"]),
    )

    # Region intersection
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end"], regions=["1:12700-13400"]
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                ["HG00280", "HG01762", "HG00280", "HG01762", "HG00280", "HG00280"]
            ),
            "pos_start": pd.Series(
                [12546, 12546, 13354, 13354, 13375, 13396], dtype=np.int32
            ),
            "pos_end": pd.Series(
                [12771, 12771, 13374, 13389, 13395, 13413], dtype=np.int32
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )
    df = v3_dataset.read_arrow(
        attrs=["sample_name", "pos_start", "pos_end"], regions=["1:12700-13400"]
    ).to_pandas()
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )

    # Regions as string
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end"], regions="1:12700-13400"
    )
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )
    df = v3_dataset.read_arrow(
        attrs=["sample_name", "pos_start", "pos_end"], regions="1:12700-13400"
    ).to_pandas()
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )

    # Regions as numpy.ndarray
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end"], regions=np.array(["1:12700-13400"])
    )
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )
    df = v3_dataset.read_arrow(
        attrs=["sample_name", "pos_start", "pos_end"], regions=np.array(["1:12700-13400"])
    ).to_pandas()
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )

    # Region and sample intersection
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12700-13400"],
        samples=["HG01762"],
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(["HG01762", "HG01762"]),
            "pos_start": pd.Series([12546, 13354], dtype=np.int32),
            "pos_end": pd.Series([12771, 13389], dtype=np.int32),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )

    # Sample only
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end"], samples=["HG01762"]
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(["HG01762", "HG01762", "HG01762"]),
            "pos_start": pd.Series([12141, 12546, 13354], dtype=np.int32),
            "pos_end": pd.Series([12277, 12771, 13389], dtype=np.int32),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_multiple_counts(v3_dataset):
    assert v3_dataset.count() == 14
    assert v3_dataset.count() == 14
    assert v3_dataset.count(regions=["1:12700-13400"]) == 6
    assert v3_dataset.count(samples=["HG00280"], regions=["1:12700-13400"]) == 4
    assert v3_dataset.count() == 14
    assert v3_dataset.count(samples=["HG01762"]) == 3
    assert v3_dataset.count(samples=["HG00280"]) == 11


def test_empty_region(v3_dataset):
    assert v3_dataset.count(regions=["12:1-1000000"]) == 0


def test_missing_sample_raises_exception(v3_dataset):
    with pytest.raises(RuntimeError):
        v3_dataset.count(samples=["abcde"])


# TODO remove skip
@pytest.mark.skip
def test_bad_contig_raises_exception(v3_dataset):
    with pytest.raises(RuntimeError):
        v3_dataset.count(regions=["chr1:1-1000000"])
    with pytest.raises(RuntimeError):
        v3_dataset.count(regions=["1"])
    with pytest.raises(RuntimeError):
        v3_dataset.count(regions=["1:100-"])
    with pytest.raises(RuntimeError):
        v3_dataset.count(regions=["1:-100"])


def test_bad_attr_raises_exception(v3_dataset):
    with pytest.raises(RuntimeError):
        v3_dataset.read(attrs=["abcde"], regions=["1:12700-13400"])


def test_read_write_mode_exceptions():
    ds = tiledbvcf.Dataset(os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples"))
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small2.bcf"]]

    with pytest.raises(Exception):
        ds.create_dataset()

    with pytest.raises(Exception):
        ds.ingest_samples(samples)

    ds = tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples"), mode="w"
    )
    with pytest.raises(Exception):
        ds.count()


def test_incomplete_reads():
    # Using undocumented "0 MB" budget to test incomplete reads.
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=0)
    v3_dataset = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    df = v3_dataset.read(attrs=["pos_end"], regions=["1:12700-13400"])
    assert not v3_dataset.read_completed()
    assert len(df) == 2
    assert_dfs_equal(
        pd.DataFrame.from_dict({"pos_end": np.array([12771, 12771], dtype=np.int32)}),
        df,
    )

    df = v3_dataset.continue_read()
    assert not v3_dataset.read_completed()
    assert len(df) == 2
    assert_dfs_equal(
        pd.DataFrame.from_dict({"pos_end": np.array([13374, 13389], dtype=np.int32)}),
        df,
    )

    df = v3_dataset.continue_read()
    assert v3_dataset.read_completed()
    assert len(df) == 2
    assert_dfs_equal(
        pd.DataFrame.from_dict({"pos_end": np.array([13395, 13413], dtype=np.int32)}),
        df,
    )

    # test incomplete via read_arrow
    table = v3_dataset.read_arrow(attrs=["pos_end"], regions=["1:12700-13400"])
    assert not v3_dataset.read_completed()
    assert len(table) == 2
    assert_dfs_equal(
        pd.DataFrame.from_dict({"pos_end": np.array([12771, 12771], dtype=np.int32)}),
        table.to_pandas(),
    )

    table = v3_dataset.continue_read_arrow()
    assert not v3_dataset.read_completed()
    assert len(table) == 2
    assert_dfs_equal(
        pd.DataFrame.from_dict({"pos_end": np.array([13374, 13389], dtype=np.int32)}),
        table.to_pandas(),
    )

    table = v3_dataset.continue_read_arrow()
    assert v3_dataset.read_completed()
    assert len(table) == 2
    assert_dfs_equal(
        pd.DataFrame.from_dict({"pos_end": np.array([13395, 13413], dtype=np.int32)}),
        table.to_pandas(),
    )


def test_incomplete_read_generator():
    # Using undocumented "0 MB" budget to test incomplete reads.
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=0)
    v3_dataset = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    expected_df = pd.DataFrame.from_dict(
            {
                "pos_end": np.array(
                    [12771, 12771, 13374, 13389, 13395, 13413], dtype=np.int32
                )
            }
        )

    # NOTE: Running multiple test shows that the iterator can be reused

    # Regions as string
    dfs = []
    for df in v3_dataset.read_iter(attrs=["pos_end"], regions="1:12700-13400"):
        dfs.append(df)
    overall_df = pd.concat(dfs, ignore_index=True)
    assert len(overall_df) == 6
    assert_dfs_equal(expected_df, overall_df)

    # Regions as list
    dfs = []
    for df in v3_dataset.read_iter(attrs=["pos_end"], regions=["1:12700-13400"]):
        dfs.append(df)
    overall_df = pd.concat(dfs, ignore_index=True)
    assert len(overall_df) == 6
    assert_dfs_equal(expected_df, overall_df)

    # Regions as numpy.ndarray
    dfs = []
    for df in v3_dataset.read_iter(attrs=["pos_end"], regions=np.array(["1:12700-13400"])):
        dfs.append(df)
    overall_df = pd.concat(dfs, ignore_index=True)
    assert len(overall_df) == 6
    assert_dfs_equal(expected_df, overall_df)


def test_read_filters(v3_dataset):
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end", "filters"],
        regions=["1:12700-13400"],
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                ["HG00280", "HG01762", "HG00280", "HG01762", "HG00280", "HG00280"]
            ),
            "pos_start": pd.Series(
                [12546, 12546, 13354, 13354, 13375, 13396], dtype=np.int32
            ),
            "pos_end": pd.Series(
                [12771, 12771, 13374, 13389, 13395, 13413], dtype=np.int32
            ),
            "filters": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=object),
                    [None, None, ["LowQual"], None, None, None],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_read_var_length_filters(tmp_path):
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["varLenFilter.vcf.gz"]]
    ds.create_dataset()
    ds.ingest_samples(samples)

    ds = tiledbvcf.Dataset(uri, mode="r")
    df = ds.read(["pos_start", "filters"])

    expected_df = pd.DataFrame(
        {
            "pos_start": pd.Series(
                [
                    12141,
                    12546,
                    13354,
                    13375,
                    13396,
                    13414,
                    13452,
                    13520,
                    13545,
                    17319,
                    17480,
                ],
                dtype=np.int32,
            ),
            "filters": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=object),
                    [
                        ["PASS"],
                        ["PASS"],
                        ["ANEUPLOID", "LowQual"],
                        ["PASS"],
                        ["PASS"],
                        ["ANEUPLOID", "LOWQ", "LowQual"],
                        ["PASS"],
                        ["PASS"],
                        ["PASS"],
                        ["LowQual"],
                        ["PASS"],
                    ],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["pos_start"])

    assert_dfs_equal(expected_df, df.sort_values(ignore_index=True, by=["pos_start"]))


def test_read_alleles(v3_dataset):
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end", "alleles"],
        regions=["1:12100-13360", "1:13500-17350"],
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                [
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                ]
            ),
            "pos_start": pd.Series(
                [12141, 12141, 12546, 12546, 13354, 13354, 13452, 13520, 13545, 17319],
                dtype=np.int32,
            ),
            "pos_end": pd.Series(
                [12277, 12277, 12771, 12771, 13374, 13389, 13519, 13544, 13689, 17479],
                dtype=np.int32,
            ),
            "alleles": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=object),
                    [
                        ["C", "<NON_REF>"],
                        ["C", "<NON_REF>"],
                        ["G", "<NON_REF>"],
                        ["G", "<NON_REF>"],
                        ["T", "<NON_REF>"],
                        ["T", "<NON_REF>"],
                        ["G", "<NON_REF>"],
                        ["G", "<NON_REF>"],
                        ["G", "<NON_REF>"],
                        ["T", "<NON_REF>"],
                    ],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_read_multiple_alleles(tmp_path):
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small3.bcf", "small.bcf"]]
    ds.create_dataset()
    ds.ingest_samples(samples)

    ds = tiledbvcf.Dataset(uri, mode="r")
    df = ds.read(
        attrs=["sample_name", "pos_start", "alleles", "id", "filters"],
        regions=["1:70100-1300000"],
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(["HG00280", "HG00280"]),
            "pos_start": pd.Series([866511, 1289367], dtype=np.int32),
            "alleles": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=object),
                    [["T", "CCCCTCCCT", "C", "CCCCTCCCTCCCT", "CCCCT"], ["CTG", "C"]],
                )
            ),
            "id": pd.Series([".", "rs1497816"]),
            "filters": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=object),
                    [["LowQual"], ["LowQual"]],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_read_var_len_attrs(v3_dataset):
    df = v3_dataset.read(
        attrs=["sample_name", "pos_start", "pos_end", "fmt_DP", "fmt_PL"],
        regions=["1:12100-13360", "1:13500-17350"],
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                [
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG01762",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                ]
            ),
            "pos_start": pd.Series(
                [12141, 12141, 12546, 12546, 13354, 13354, 13452, 13520, 13545, 17319],
                dtype=np.int32,
            ),
            "pos_end": pd.Series(
                [12277, 12277, 12771, 12771, 13374, 13389, 13519, 13544, 13689, 17479],
                dtype=np.int32,
            ),
            "fmt_DP": pd.Series([0, 0, 0, 0, 15, 64, 10, 6, 0, 0], dtype=np.int32),
            "fmt_PL": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=np.int32),
                    [
                        [0, 0, 0],
                        [0, 0, 0],
                        [0, 0, 0],
                        [0, 0, 0],
                        [0, 24, 360],
                        [0, 66, 990],
                        [0, 21, 210],
                        [0, 6, 90],
                        [0, 0, 0],
                        [0, 0, 0],
                    ],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])

    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_sample_args(v3_dataset, tmp_path):
    sample_file = os.path.join(tmp_path, "1_sample.txt")
    with open(sample_file, "w") as file:
        file.write("HG00280")

    region = ["1:12141-12141"]
    df1 = v3_dataset.read(["sample_name"], regions=region, samples=["HG00280"])
    df2 = v3_dataset.read(["sample_name"], regions=region, samples_file=sample_file)
    assert_dfs_equal(df1, df2)

    with pytest.raises(TypeError):
        v3_dataset.read(
            attrs=["sample_name"],
            regions=region,
            samples=["HG00280"],
            samples_file=sample_file,
        )


def test_read_null_attrs(tmp_path):
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small3.bcf", "small.bcf"]]
    ds.create_dataset()
    ds.ingest_samples(samples)

    ds = tiledbvcf.Dataset(uri, mode="r")
    df = ds.read(
        attrs=[
            "sample_name",
            "pos_start",
            "pos_end",
            "info_BaseQRankSum",
            "info_DP",
            "fmt_DP",
            "fmt_MIN_DP",
        ],
        regions=["1:12700-13400", "1:69500-69800"],
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(
                [
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG01762",
                    "HG01762",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                    "HG00280",
                ]
            ),
            "pos_start": pd.Series(
                [
                    12546,
                    13354,
                    13375,
                    13396,
                    12546,
                    13354,
                    69371,
                    69511,
                    69512,
                    69761,
                    69762,
                    69771,
                ],
                dtype=np.int32,
            ),
            "pos_end": pd.Series(
                [
                    12771,
                    13374,
                    13395,
                    13413,
                    12771,
                    13389,
                    69510,
                    69511,
                    69760,
                    69761,
                    69770,
                    69834,
                ],
                dtype=np.int32,
            ),
            "info_BaseQRankSum": pd.Series(
                [
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    np.array([-0.787], dtype=np.float32),
                    None,
                    np.array([1.97], dtype=np.float32),
                    None,
                    None,
                ]
            ),
            "info_DP": pd.Series(
                [
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    np.array([89], dtype=np.int32),
                    None,
                    np.array([24], dtype=np.int32),
                    None,
                    None,
                ]
            ),
            "fmt_DP": pd.Series(
                [0, 15, 6, 2, 0, 64, 180, 88, 97, 24, 23, 21], dtype=np.int32
            ),
            "fmt_MIN_DP": pd.Series([0, 14, 3, 1, 0, 30, 20, None, 24, None, 23, 19]),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    assert_dfs_equal(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_read_config():
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig()
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    cfg = tiledbvcf.ReadConfig(
        memory_budget_mb=512,
        region_partition=(0, 3),
        tiledb_config=["sm.tile_cache_size=0", "sm.compute_concurrency_level=1"],
    )
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    with pytest.raises(TypeError):
        cfg = tiledbvcf.ReadConfig(abc=123)

    # Expect an exception when passing both cfg and tiledb_config
    with pytest.raises(Exception):
        cfg = tiledbvcf.ReadConfig()
        tiledb_config = {"foo": "bar"}
        ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg, tiledb_config=tiledb_config)


# This test is skipped because running it in the same process as all the normal
# tests will cause it to fail (the first context created in a process determines
# the number of TBB threads allowed).
@pytest.mark.skip
def test_tbb_threads_config():
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(tiledb_config=["sm.num_tbb_threads=3"])
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    cfg = tiledbvcf.ReadConfig(tiledb_config=["sm.num_tbb_threads=4"])
    with pytest.raises(RuntimeError):
        ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)


def test_read_limit():
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(limit=3)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end", "fmt_DP", "fmt_PL"],
        regions=["1:12100-13360", "1:13500-17350"],
    )
    assert len(df) == 3


def test_region_partitioned_read():
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")

    cfg = tiledbvcf.ReadConfig(region_partition=(0, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 4

    cfg = tiledbvcf.ReadConfig(region_partition=(1, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 2

    # Too many partitions still produces results
    cfg = tiledbvcf.ReadConfig(region_partition=(1, 3))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 2

    # Error: index >= num partitions
    cfg = tiledbvcf.ReadConfig(region_partition=(2, 2))
    with pytest.raises(RuntimeError):
        ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)


def test_sample_partitioned_read():
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")

    cfg = tiledbvcf.ReadConfig(sample_partition=(0, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"], regions=["1:12000-18000"]
    )
    assert len(df) == 11
    assert (df.sample_name == "HG00280").all()

    cfg = tiledbvcf.ReadConfig(sample_partition=(1, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"], regions=["1:12000-18000"]
    )
    assert len(df) == 3
    assert (df.sample_name == "HG01762").all()

    # Error: too many partitions
    cfg = tiledbvcf.ReadConfig(sample_partition=(1, 3))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    with pytest.raises(RuntimeError):
        df = ds.read(
            attrs=["sample_name", "pos_start", "pos_end"], regions=["1:12000-18000"]
        )

    # Error: index >= num partitions
    cfg = tiledbvcf.ReadConfig(sample_partition=(2, 2))
    with pytest.raises(RuntimeError):
        ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)


def test_sample_and_region_partitioned_read():
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")

    cfg = tiledbvcf.ReadConfig(region_partition=(0, 2), sample_partition=(0, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 2
    assert (df.sample_name == "HG00280").all()

    cfg = tiledbvcf.ReadConfig(region_partition=(0, 2), sample_partition=(1, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 2
    assert (df.sample_name == "HG01762").all()

    cfg = tiledbvcf.ReadConfig(region_partition=(1, 2), sample_partition=(0, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 2
    assert (df.sample_name == "HG00280").all()

    cfg = tiledbvcf.ReadConfig(region_partition=(1, 2), sample_partition=(1, 2))
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end"],
        regions=["1:12000-13000", "1:17000-18000"],
    )
    assert len(df) == 0


@pytest.mark.skipif(os.environ.get("CI") != "true", reason="CI only")
def test_large_export_correctness():
    uri = "s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20"

    ds = tiledbvcf.Dataset(uri)
    df = ds.read(
        attrs=[
            "sample_name",
            "contig",
            "pos_start",
            "pos_end",
            "query_bed_start",
            "query_bed_end",
        ],
        samples=["v2-DjrIAzkP", "v2-YMaDHIoW", "v2-usVwJUmo", "v2-ZVudhauk"],
        bed_file=os.path.join(
            TESTS_INPUT_DIR, "E001_15_coreMarks_dense_filtered.bed.gz"
        ),
    )

    # total number of exported records
    assert df.shape[0] == 1172081

    # number of unique exported records
    record_index = ["sample_name", "contig", "pos_start"]
    assert df[record_index].drop_duplicates().shape[0] == 1168430


def test_basic_ingest(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small2.bcf"]]
    ds.create_dataset()
    ds.ingest_samples(samples)

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 14
    assert ds.count(regions=["1:12700-13400"]) == 6
    assert ds.count(samples=["HG00280"], regions=["1:12700-13400"]) == 4


def test_disable_ingestion_tasks(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small3.bcf"]]
    ds.create_dataset(
        enable_allele_count=False, enable_variant_stats=False, enable_sample_stats=False
    )
    ds.ingest_samples(samples)

    # TODO: remove this workaround when sc-19721 is resolved
    if platform.system() != "Linux":
        return

    # Validate that stats arrays were not created
    ac_uri = os.path.join(tmp_path, "dataset", "allele_count")
    vs_uri = os.path.join(tmp_path, "dataset", "variant_stats")
    ss_uri = os.path.join(tmp_path, "dataset", "sample_stats")

    assert not os.path.exists(ac_uri)
    assert not os.path.exists(vs_uri)
    assert not os.path.exists(ss_uri)


def test_ingestion_tasks(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small3.bcf"]]
    ds.create_dataset(enable_allele_count=True, enable_variant_stats=True)
    ds.ingest_samples(samples)

    # TODO: remove this workaround when sc-19721 is resolved
    if platform.system() != "Linux":
        return

    # query allele_count array with TileDB
    ac_uri = tiledb.Group(uri)["allele_count"].uri

    skip_if_incompatible(ac_uri)

    contig = "1"
    region = slice(69896)
    with tiledb.open(ac_uri) as A:
        df = A.query(attrs=["alt", "count"], dims=["pos"]).df[contig, region]

    assert df["pos"].array == 69896
    assert df["alt"].array == "C"
    assert df["count"].array == 1

    # query variant_stats array with TileDB
    vs_uri = tiledb.Group(uri)["variant_stats"].uri

    contig = "1"
    region = slice(12140)
    with tiledb.open(vs_uri) as A:
        df = A.query(attrs=["allele", "ac"], dims=["pos"]).df[contig, region]

    assert df["pos"].array == 12140
    assert df["allele"].array == "C"
    assert df["ac"].array == 4

    # Test raw sample_stats

    expected_df = pd.DataFrame(
        {
            "sample": ["HG00280", "HG01762"],
            "dp_sum": [879, 64],
            "dp_sum2": [56375, 4096],
            "dp_count": [68, 2],
            "dp_min": [0, 0],
            "dp_max": [180, 64],
            "gq_sum": [1489, 99],
            "gq_sum2": [79129, 9801],
            "gq_count": [68, 2],
            "gq_min": [0, 0],
            "gq_max": [99, 99],
            "n_records": [70, 3],
            "n_called": [70, 3],
            "n_not_called": [0, 0],
            "n_hom_ref": [64, 3],
            "n_het": [3, 0],
            "n_singleton": [4, 0],
            "n_snp": [7, 0],
            "n_insertion": [2, 0],
            "n_deletion": [1, 0],
            "n_transition": [6, 0],
            "n_transversion": [1, 0],
            "n_star": [0, 0],
            "n_multiallelic": [5, 0],
        }
    ).astype("uint64", errors="ignore")

    ss_uri = tiledb.Group(uri)["sample_stats"].uri
    with tiledb.open(ss_uri) as A:
        df = A.df[:]

    # Convert to uint64 for comparison to expected_df
    df = df.astype("uint64", errors="ignore")

    assert df.equals(expected_df)

    # Test sample_qc
    expected_qc = pd.DataFrame(
        {
            "sample": ["HG00280", "HG01762"],
            "dp_mean": [12.92647, 32.0],
            "dp_stddev": [25.728399, 32.0],
            "dp_min": [0, 0],
            "dp_max": [180, 64],
            "gq_mean": [21.897058, 49.5],
            "gq_stddev": [26.156845, 49.5],
            "gq_min": [0, 0],
            "gq_max": [99, 99],
            "call_rate": [1.0, 1.0],
            "n_called": [70, 3],
            "n_not_called": [0, 0],
            "n_hom_ref": [64, 3],
            "n_het": [3, 0],
            "n_hom_var": [3, 0],
            "n_non_ref": [6, 0],
            "n_singleton": [4, 0],
            "n_snp": [7, 0],
            "n_insertion": [2, 0],
            "n_deletion": [1, 0],
            "n_transition": [6, 0],
            "n_transversion": [1, 0],
            "n_star": [0, 0],
            "r_ti_tv": [6.0, np.nan],
            "r_het_hom_var": [1.0, np.nan],
            "r_insertion_deletion": [2.0, np.nan],
            "n_records": [70, 3],
            "n_multiallelic": [5, 0],
        }
    )

    qc = tiledbvcf.sample_qc(uri)
    assert_dfs_equal(expected_qc, qc)


def test_incremental_ingest(tmp_path):
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small2.bcf")])

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 14
    assert ds.count(regions=["1:12700-13400"]) == 6
    assert ds.count(samples=["HG00280"], regions=["1:12700-13400"]) == 4


def test_ingest_disable_merging(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset_disable_merging")

    cfg = tiledbvcf.ReadConfig(memory_budget_mb=1024)
    attrs = ["sample_name", "contig", "pos_start", "pos_end"]

    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds.create_dataset()
    ds.ingest_samples(samples, contig_fragment_merging=False)

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, cfg=cfg, mode="r", verbose=False)
    df = ds.read(attrs=attrs)
    assert ds.count() == 246
    assert ds.count(regions=["chrX:9032893-9032893"]) == 1

    # Create the dataset
    uri = os.path.join(tmp_path, "dataset_merging_separate")
    ds2 = tiledbvcf.Dataset(uri, mode="w", verbose=False)
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds2.create_dataset()
    ds2.ingest_samples(samples, contigs_to_keep_separate=["chr1"])

    # Open it back in read mode and check some queries
    ds2 = tiledbvcf.Dataset(uri, cfg=cfg, mode="r", verbose=False)
    df2 = ds2.read(attrs=attrs)
    assert df.equals(df2)

    assert ds.count() == 246
    assert ds.count(regions=["chrX:9032893-9032893"]) == 1


def test_ingest_merging_separate(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset_merging_separate")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds.create_dataset()
    ds.ingest_samples(samples, contigs_to_keep_separate=["chr1"])

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 246
    assert ds.count(regions=["chrX:9032893-9032893"]) == 1


def test_ingest_merging(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset_merging")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds.create_dataset()
    ds.ingest_samples(samples, contigs_to_allow_merging=["chr1", "chr2"])

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 246
    assert ds.count(regions=["chrX:9032893-9032893"]) == 1


def test_ingest_mode_merged(tmp_path):
    # tiledbvcf.config_logging("debug")
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset_merging")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds.create_dataset()
    # ingest only merged contigs (pseudo-contigs)
    ds.ingest_samples(samples, contig_mode="merged")

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 19
    assert ds.count(regions=["chrX:9032893-9032893"]) == 0


# Shared marker for all v3 stats tests — bcftools may be absent on Windows CI
_skip_if_no_bcftools = pytest.mark.skipif(
    os.environ.get("CI") == "true"
    and platform.system() == "Windows"
    and shutil.which("bcftools") is None,
    reason="no bcftools",
)


@_skip_if_no_bcftools
def test_read_with_af_filter(stats_v3_dataset, stats_sample_names):
    attrs = ["contig", "pos_start", "id", "qual", "info_TILEDB_IAF", "sample_name"]
    df = stats_v3_dataset.read(
        samples=stats_sample_names,
        attrs=attrs,
        set_af_filter="<0.2",
    )
    assert df.shape == (1, 8)
    assert df.query("sample_name == 'second'")["qual"].iloc[0] == pytest.approx(343.73)
    assert df[df["sample_name"] == "second"]["info_TILEDB_IAF"].iloc[0][0] == 0.9375


@_skip_if_no_bcftools
def test_read_with_scan_all_samples(stats_v3_dataset, stats_sample_names):
    attrs = ["contig", "pos_start", "id", "qual", "info_TILEDB_IAF", "sample_name"]
    df = stats_v3_dataset.read(
        samples=stats_sample_names,
        attrs=attrs,
        scan_all_samples=True,
    )
    assert (
        df[(df["sample_name"] == "second") & (df["pos_start"] == 4)][
            "info_TILEDB_IAF"
        ].iloc[0][0]
        == 0.9375
    )


@_skip_if_no_bcftools
def test_variant_stats_parameter_errors(stats_v3_dataset):
    no_region = '"region" or "regions" parameter is required'
    exclusive = '"region" and "regions" parameters are mutually exclusive'
    bad_format = '"region" parameter must have format "<contig>:<start>-<end>"'
    empty_contig = "Region contig cannot be empty"
    base_1 = "Regions must be 1-based"
    bad_interval = '"100-1" is not a valid region interval'

    for fn in [stats_v3_dataset.read_variant_stats, stats_v3_dataset.read_variant_stats_arrow]:
        with pytest.raises(Exception, match=no_region):
            fn()
        with pytest.raises(Exception, match=exclusive):
            fn("chr1:1-100", regions=["chr1:1-100"])
        with pytest.raises(Exception, match=bad_format):
            fn(regions=[""])
        with pytest.raises(Exception, match=bad_format):
            fn(regions=["chr1"])
        with pytest.raises(Exception, match=bad_format):
            fn(regions=["chr1:-"])
        with pytest.raises(Exception, match=empty_contig):
            fn(regions=[":1-100"])
        with pytest.raises(Exception, match=base_1):
            fn(regions=["chr1:0-100"])
        with pytest.raises(Exception, match=bad_interval):
            fn(regions=["chr1:100-1"])


@_skip_if_no_bcftools
def test_variant_stats_empty_region(stats_v3_dataset):
    assert stats_v3_dataset.read_variant_stats(regions=["chr3:1-10000"]).empty


@_skip_if_no_bcftools
def test_variant_stats_return_types(stats_v3_dataset):
    # Both the deprecated positional `region` parameter and the `regions` list
    # should return a DataFrame / Arrow Table of the same shape and content.
    region = "chr1:1-10000"
    for kwargs in [{"region": region}, {"regions": [region]}]:
        # Workaround: read_variant_stats takes region as positional-or-keyword
        if "region" in kwargs:
            df = stats_v3_dataset.read_variant_stats(kwargs["region"])
            tbl = stats_v3_dataset.read_variant_stats_arrow(kwargs["region"])
        else:
            df = stats_v3_dataset.read_variant_stats(**kwargs)
            tbl = stats_v3_dataset.read_variant_stats_arrow(**kwargs)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(tbl, pa.Table)
        assert df.shape == (13, 6)
        assert df.equals(tbl.to_pandas())


@_skip_if_no_bcftools
def test_variant_stats_multi_contig_regions(stats_v3_dataset):
    # Results are always returned in contig-sorted order regardless of input order.
    region_chr1 = "chr1:1-10000"
    region_chr2 = "chr2:1-10000"
    expected_contigs = ["chr1"] * 13 + ["chr2"] * 2

    df = stats_v3_dataset.read_variant_stats(regions=[region_chr1, region_chr2])
    assert df.shape == (15, 6)
    assert expected_contigs == list(df["contig"].values)

    df_reversed = stats_v3_dataset.read_variant_stats(regions=[region_chr2, region_chr1])
    assert df.equals(df_reversed)

    tbl = stats_v3_dataset.read_variant_stats_arrow(regions=[region_chr1, region_chr2])
    tbl_reversed = stats_v3_dataset.read_variant_stats_arrow(regions=[region_chr2, region_chr1])
    assert tbl.equals(tbl_reversed)
    assert df.equals(tbl.to_pandas())


@_skip_if_no_bcftools
def test_variant_stats_overlapping_regions(stats_v3_dataset):
    # Overlapping regions on the same contig are merged; results are deduped and sorted.
    expected_contigs = ["chr1"] * 13 + ["chr2"] * 2

    assert stats_v3_dataset.read_variant_stats(regions=["chr1:1-1"]).shape == (2, 6)
    assert stats_v3_dataset.read_variant_stats(regions=["chr1:1-2"]).shape == (5, 6)
    assert stats_v3_dataset.read_variant_stats(regions=["chr1:3-4"]).shape == (6, 6)
    assert stats_v3_dataset.read_variant_stats(regions=["chr1:2-5"]).shape == (11, 6)

    regions_chr1 = ["chr1:1-1", "chr1:1-2", "chr1:3-4", "chr1:2-5"]
    df = stats_v3_dataset.read_variant_stats(regions=regions_chr1)
    assert df.shape == (13, 6)
    assert df.equals(stats_v3_dataset.read_variant_stats(regions=reversed(regions_chr1)))

    assert stats_v3_dataset.read_variant_stats(regions=["chr2:1-1"]).shape == (1, 6)
    assert stats_v3_dataset.read_variant_stats(regions=["chr2:3-3"]).shape == (1, 6)

    regions_chr2 = ["chr2:1-1", "chr2:3-3"]
    df = stats_v3_dataset.read_variant_stats(regions=regions_chr2)
    assert df.shape == (2, 6)
    assert df.equals(stats_v3_dataset.read_variant_stats(regions=reversed(regions_chr2)))

    for regions in [regions_chr1 + regions_chr2, regions_chr2 + regions_chr1]:
        df = stats_v3_dataset.read_variant_stats(regions=regions)
        assert df.shape == (15, 6)
        assert expected_contigs == list(df["contig"].values)
        assert df.equals(stats_v3_dataset.read_variant_stats(regions=reversed(regions)))


@_skip_if_no_bcftools
def test_variant_stats_scan_all_samples(stats_v3_dataset):
    # Without scan_all_samples, an reflects only the queried samples' allele number.
    # With scan_all_samples=True, an is normalised across all samples in the dataset.
    regions = ["chr2:1-1", "chr2:3-3", "chr1:1-1", "chr1:1-2", "chr1:3-4", "chr1:2-5"]
    ac = [8, 8, 5, 6, 5, 4, 4, 4, 4, 1, 15, 1, 2, 2, 2]

    df = stats_v3_dataset.read_variant_stats(regions=regions)
    assert ac == list(df["ac"].values)
    assert [16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 3, 3, 2, 2] == list(df["an"].values)
    assert [0.5, 0.5, 0.3125, 0.375, 0.3125, 0.25, 0.25, 0.25, 0.25, 0.0625, 0.9375,
            0.33333334, 0.6666667, 1.0, 1.0] == list(df["af"].values)

    df = stats_v3_dataset.read_variant_stats(regions=regions, scan_all_samples=True)
    assert ac == list(df["ac"].values)
    assert [16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16] == list(df["an"].values)
    assert [0.5, 0.5, 0.3125, 0.375, 0.3125, 0.25, 0.25, 0.25, 0.25, 0.0625, 0.9375,
            0.0625, 0.125, 0.125, 0.125] == list(df["af"].values)


@_skip_if_no_bcftools
def test_variant_stats_drop_ref(stats_v3_dataset):
    # drop_ref=True filters out rows where the alternate allele is "ref".
    regions = ["chr2:1-1", "chr2:3-3", "chr1:1-1", "chr1:1-2", "chr1:3-4", "chr1:2-5"]

    df = stats_v3_dataset.read_variant_stats(regions=regions)
    assert ["T,C", "ref", "G,GTTTA", "G,T", "ref", "C,A", "C,G", "C,T", "ref",
            "G,GTTTA", "ref", "C,T", "ref", "G,GTTTA", "G,GTTTA"] == list(df["alleles"].values)

    df = stats_v3_dataset.read_variant_stats(regions=regions, drop_ref=True)
    assert ["T,C", "G,GTTTA", "G,T", "C,A", "C,G", "C,T", "G,GTTTA",
            "C,T", "G,GTTTA", "G,GTTTA"] == list(df["alleles"].values)


@_skip_if_no_bcftools
def test_allele_count_parameter_errors(stats_v3_dataset):
    no_region = '"region" or "regions" parameter is required'
    exclusive = '"region" and "regions" parameters are mutually exclusive'
    bad_format = '"region" parameter must have format "<contig>:<start>-<end>"'
    empty_contig = "Region contig cannot be empty"
    base_1 = "Regions must be 1-based"
    bad_interval = '"100-1" is not a valid region interval'

    for fn in [stats_v3_dataset.read_allele_count, stats_v3_dataset.read_allele_count_arrow]:
        with pytest.raises(Exception, match=no_region):
            fn()
        with pytest.raises(Exception, match=exclusive):
            fn("chr1:1-100", regions=["chr1:1-100"])
        with pytest.raises(Exception, match=bad_format):
            fn(regions=[""])
        with pytest.raises(Exception, match=bad_format):
            fn(regions=["chr1"])
        with pytest.raises(Exception, match=bad_format):
            fn(regions=["chr1:-"])
        with pytest.raises(Exception, match=empty_contig):
            fn(regions=[":1-100"])
        with pytest.raises(Exception, match=base_1):
            fn(regions=["chr1:0-100"])
        with pytest.raises(Exception, match=bad_interval):
            fn(regions=["chr1:100-1"])


@_skip_if_no_bcftools
def test_allele_count_empty_region(stats_v3_dataset):
    assert stats_v3_dataset.read_allele_count(regions=["chr3:1-10000"]).empty


@_skip_if_no_bcftools
def test_allele_count_return_types(stats_v3_dataset):
    # Both the deprecated positional `region` parameter and the `regions` list
    # should return a DataFrame / Arrow Table of the same shape and content.
    region = "chr1:1-10000"
    expected_pos = (0, 1, 1, 2, 2, 2, 3)
    expected_count = (8, 5, 3, 4, 2, 2, 1)

    for kwargs in [{"region": region}, {"regions": [region]}]:
        if "region" in kwargs:
            df = stats_v3_dataset.read_allele_count(kwargs["region"])
            tbl = stats_v3_dataset.read_allele_count_arrow(kwargs["region"])
        else:
            df = stats_v3_dataset.read_allele_count(**kwargs)
            tbl = stats_v3_dataset.read_allele_count_arrow(**kwargs)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(tbl, pa.Table)
        assert df.shape == (7, 7)
        assert df.equals(tbl.to_pandas())
        assert sum(df["pos"] == expected_pos) == 7
        assert sum(df["count"] == expected_count) == 7


@_skip_if_no_bcftools
def test_allele_count_multi_contig_regions(stats_v3_dataset):
    # Results are always returned in contig-sorted order regardless of input order.
    region_chr1 = "chr1:1-10000"
    region_chr2 = "chr2:1-10000"
    expected_contigs = ["chr1"] * 7 + ["chr2"] * 2

    df = stats_v3_dataset.read_allele_count(regions=[region_chr1, region_chr2])
    assert df.shape == (9, 7)
    assert expected_contigs == list(df["contig"].values)

    df_reversed = stats_v3_dataset.read_allele_count(regions=[region_chr2, region_chr1])
    assert df.equals(df_reversed)

    tbl = stats_v3_dataset.read_allele_count_arrow(regions=[region_chr1, region_chr2])
    tbl_reversed = stats_v3_dataset.read_allele_count_arrow(regions=[region_chr2, region_chr1])
    assert tbl.equals(tbl_reversed)
    assert df.equals(tbl.to_pandas())


@_skip_if_no_bcftools
def test_allele_count_overlapping_regions(stats_v3_dataset):
    # Overlapping regions on the same contig are merged; results are deduped and sorted.
    expected_contigs = ["chr1"] * 7 + ["chr2"] * 2

    assert stats_v3_dataset.read_allele_count(regions=["chr1:1-1"]).shape == (1, 7)
    assert stats_v3_dataset.read_allele_count(regions=["chr1:1-2"]).shape == (3, 7)
    assert stats_v3_dataset.read_allele_count(regions=["chr1:3-4"]).shape == (4, 7)
    assert stats_v3_dataset.read_allele_count(regions=["chr1:2-5"]).shape == (6, 7)

    regions_chr1 = ["chr1:1-1", "chr1:1-2", "chr1:3-4", "chr1:2-5"]
    df = stats_v3_dataset.read_allele_count(regions=regions_chr1)
    assert df.shape == (7, 7)
    assert df.equals(stats_v3_dataset.read_allele_count(regions=reversed(regions_chr1)))

    assert stats_v3_dataset.read_allele_count(regions=["chr2:1-1"]).shape == (1, 7)
    assert stats_v3_dataset.read_allele_count(regions=["chr2:3-3"]).shape == (1, 7)

    regions_chr2 = ["chr2:1-1", "chr2:3-3"]
    df = stats_v3_dataset.read_allele_count(regions=regions_chr2)
    assert df.shape == (2, 7)
    assert df.equals(stats_v3_dataset.read_allele_count(regions=reversed(regions_chr2)))

    for regions in [regions_chr1 + regions_chr2, regions_chr2 + regions_chr1]:
        df = stats_v3_dataset.read_allele_count(regions=regions)
        assert df.shape == (9, 7)
        assert expected_contigs == list(df["contig"].values)
        assert df.equals(stats_v3_dataset.read_allele_count(regions=reversed(regions)))


@_skip_if_no_bcftools
def test_allele_frequency(stats_v3_dataset, tmp_path):
    # Verify that ac / af ≈ an (i.e. allele frequency is consistent with counts).
    region = "chr1:1-10000"
    df = tiledbvcf.allele_frequency.read_allele_frequency(
        os.path.join(tmp_path, "stats_test"), region
    )
    assert df.pos.is_monotonic_increasing
    df["an_check"] = (df.ac / df.af).round(0).astype("int32")
    assert df.an_check.equals(df.an)
    assert stats_v3_dataset.read_variant_stats(region).shape == (13, 6)


@pytest.mark.skipif(
    os.environ.get("CI") == "true"
    and platform.system() == "Windows"
    and shutil.which("bcftools") is None,
    reason="no bcftools",
)
def test_delete_samples(tmp_path, stats_v3_dataset, stats_sample_names):
    #    assert stats_v3_dataset.samples() == stats_sample_names
    assert "second" in stats_sample_names
    assert "fifth" in stats_sample_names
    assert "third" in stats_sample_names
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="w")
    # tiledbvcf.config_logging("trace")
    ds.delete_samples(["second", "fifth"])
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="r")
    sample_names = ds.samples()
    assert "second" not in sample_names
    assert "fifth" not in sample_names
    assert "third" in sample_names


# Ok to skip is missing bcftools in Windows CI job
@pytest.mark.skipif(
    os.environ.get("CI") == "true"
    and platform.system() == "Windows"
    and shutil.which("bcftools") is None,
    reason="no bcftools",
)
def test_ingest_with_stats_v2(tmp_path, bgzip_and_index_vcfs):
    # tiledbvcf.config_logging("debug")
    shutil.copytree(
        os.path.join(TESTS_INPUT_DIR, "stats"), os.path.join(tmp_path, "stats")
    )
    bgzipped_inputs = bgzip_and_index_vcfs(os.path.join(tmp_path, "stats"))
    # tiledbvcf.config_logging("trace")
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="w")
    ds.create_dataset(enable_variant_stats=True, enable_allele_count=True)
    ds.ingest_samples(bgzipped_inputs)
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="r")
    sample_names = [os.path.basename(file).split(".")[0] for file in bgzipped_inputs]
    data_frame = ds.read(
        samples=sample_names,
        attrs=["contig", "pos_start", "id", "qual", "info_TILEDB_IAF", "sample_name"],
        set_af_filter="<0.2",
    )
    assert data_frame.shape == (1, 8)
    assert data_frame.query("sample_name == 'second'")["qual"].iloc[0] == pytest.approx(
        343.73
    )
    assert (
        data_frame[data_frame["sample_name"] == "second"]["info_TILEDB_IAF"].iloc[0][0]
        == 0.9375
    )
    data_frame = ds.read(
        samples=sample_names,
        attrs=["contig", "pos_start", "id", "qual", "info_TILEDB_IAF", "sample_name"],
        scan_all_samples=True,
    )
    assert (
        data_frame[
            (data_frame["sample_name"] == "second") & (data_frame["pos_start"] == 4)
        ]["info_TILEDB_IAF"].iloc[0][0]
        == 0.9375
    )
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="r")
    df = ds.read_variant_stats("chr1:1-10000")
    assert df.shape == (13, 6)
    df = tiledbvcf.allele_frequency.read_allele_frequency(
        os.path.join(tmp_path, "stats_test"), "chr1:1-10000"
    )
    assert df.pos.is_monotonic_increasing
    df["an_check"] = (df.ac / df.af).round(0).astype("int32")
    assert df.an_check.equals(df.an)
    df = ds.read_variant_stats("chr1:1-10000")
    assert df.shape == (13, 6)
    df = ds.read_allele_count("chr1:1-10000")
    assert df.shape == (7, 7)
    assert sum(df["pos"] == (0, 1, 1, 2, 2, 2, 3)) == 7
    assert sum(df["count"] == (8, 5, 3, 4, 2, 2, 1)) == 7


# Ok to skip is missing bcftools in Windows CI job
@pytest.mark.skipif(
    os.environ.get("CI") == "true"
    and platform.system() == "Windows"
    and shutil.which("bcftools") is None,
    reason="no bcftools",
)
def test_ingest_polyploid(tmp_path, bgzip_and_index_vcfs):
    shutil.copytree(
        os.path.join(TESTS_INPUT_DIR, "polyploid"), os.path.join(tmp_path, "polyploid")
    )
    bgzipped_inputs = bgzip_and_index_vcfs(os.path.join(tmp_path, "polyploid"))
    # tiledbvcf.config_logging("trace")
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "polyploid_test"), mode="w")
    ds.create_dataset(enable_variant_stats=True)
    ds.ingest_samples(bgzipped_inputs)
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "polyploid_test"), mode="r")
    sample_names = [os.path.basename(file).split(".")[0] for file in bgzipped_inputs]
    data_frame = ds.read(
        samples=sample_names,
        attrs=["contig", "pos_start", "id", "qual", "info_TILEDB_IAF", "sample_name"],
        set_af_filter="<0.8",
    )
    # print(data_frame)


def test_ingest_mode_separate(tmp_path):
    # tiledbvcf.config_logging("debug")
    # Create the dataset
    uri = os.path.join(tmp_path, "dataset_merging")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds.create_dataset()
    # ingest only merged contigs (pseudo-contigs)
    ds.ingest_samples(
        samples, contigs_to_keep_separate=["chr1"], contig_mode="separate"
    )

    # Open it back in read mode and check some queries
    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 17
    assert ds.count(regions=["chrX:9032893-9032893"]) == 0


def test_vcf_attrs(tmp_path):
    # Create the dataset with vcf info and fmt attributes
    uri = os.path.join(tmp_path, "vcf_attrs_dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    vcf_uri = os.path.join(TESTS_INPUT_DIR, "v2-DjrIAzkP-downsampled.vcf.gz")
    ds.create_dataset(vcf_attrs=vcf_uri)

    # Open it back in read mode and check attributes
    ds = tiledbvcf.Dataset(uri, mode="r")

    queryable_attrs = [
        "alleles",
        "contig",
        "filters",
        "fmt",
        "fmt_DP",
        "fmt_GQ",
        "fmt_GT",
        "fmt_MIN_DP",
        "fmt_PS",
        "fmt_SB",
        "fmt_STR_MAX_LEN",
        "fmt_STR_PERIOD",
        "fmt_STR_TIMES",
        "fmt_VAR_CONTEXT",
        "fmt_VAR_TYPE",
        "id",
        "info",
        "info_AC",
        "info_AC_AFR",
        "info_AC_AMR",
        "info_AC_Adj",
        "info_AC_CONSANGUINEOUS",
        "info_AC_EAS",
        "info_AC_FEMALE",
        "info_AC_FIN",
        "info_AC_Hemi",
        "info_AC_Het",
        "info_AC_Hom",
        "info_AC_MALE",
        "info_AC_NFE",
        "info_AC_OTH",
        "info_AC_POPMAX",
        "info_AC_SAS",
        "info_AF",
        "info_AF_AFR",
        "info_AF_AMR",
        "info_AF_Adj",
        "info_AF_EAS",
        "info_AF_FIN",
        "info_AF_NFE",
        "info_AF_OTH",
        "info_AF_SAS",
        "info_AGE_HISTOGRAM_HET",
        "info_AGE_HISTOGRAM_HOM",
        "info_AN",
        "info_AN_AFR",
        "info_AN_AMR",
        "info_AN_Adj",
        "info_AN_CONSANGUINEOUS",
        "info_AN_EAS",
        "info_AN_FEMALE",
        "info_AN_FIN",
        "info_AN_MALE",
        "info_AN_NFE",
        "info_AN_OTH",
        "info_AN_POPMAX",
        "info_AN_SAS",
        "info_BaseQRankSum",
        "info_CCC",
        "info_CSQ",
        "info_ClippingRankSum",
        "info_DB",
        "info_DOUBLETON_DIST",
        "info_DP",
        "info_DP_HIST",
        "info_DS",
        "info_END",
        "info_ESP_AC",
        "info_ESP_AF_GLOBAL",
        "info_ESP_AF_POPMAX",
        "info_FS",
        "info_GQ_HIST",
        "info_GQ_MEAN",
        "info_GQ_STDDEV",
        "info_HWP",
        "info_HaplotypeScore",
        "info_Hemi_AFR",
        "info_Hemi_AMR",
        "info_Hemi_EAS",
        "info_Hemi_FIN",
        "info_Hemi_NFE",
        "info_Hemi_OTH",
        "info_Hemi_SAS",
        "info_Het_AFR",
        "info_Het_AMR",
        "info_Het_EAS",
        "info_Het_FIN",
        "info_Het_NFE",
        "info_Het_OTH",
        "info_Het_SAS",
        "info_Hom_AFR",
        "info_Hom_AMR",
        "info_Hom_CONSANGUINEOUS",
        "info_Hom_EAS",
        "info_Hom_FIN",
        "info_Hom_NFE",
        "info_Hom_OTH",
        "info_Hom_SAS",
        "info_InbreedingCoeff",
        "info_K1_RUN",
        "info_K2_RUN",
        "info_K3_RUN",
        "info_KG_AC",
        "info_KG_AF_GLOBAL",
        "info_KG_AF_POPMAX",
        "info_MLEAC",
        "info_MLEAF",
        "info_MQ",
        "info_MQ0",
        "info_MQRankSum",
        "info_NCC",
        "info_NEGATIVE_TRAIN_SITE",
        "info_OLD_VARIANT",
        "info_POPMAX",
        "info_POSITIVE_TRAIN_SITE",
        "info_QD",
        "info_ReadPosRankSum",
        "info_VQSLOD",
        "info_clinvar_conflicted",
        "info_clinvar_measureset_id",
        "info_clinvar_mut",
        "info_clinvar_pathogenic",
        "info_culprit",
        "pos_end",
        "pos_start",
        "qual",
        "query_bed_end",
        "query_bed_line",
        "query_bed_start",
        "sample_name",
    ]

    assert ds.attributes(attr_type="info") == []
    assert ds.attributes(attr_type="fmt") == []
    assert sorted(ds.attributes()) == sorted(queryable_attrs)


@pytest.mark.parametrize("compress", [True, False])
def test_sample_compression(tmp_path, compress):
    # Create the dataset
    dataset_uri = os.path.join(tmp_path, "sample_compression")
    array_uri = os.path.join(dataset_uri, "data")
    ds = tiledbvcf.Dataset(dataset_uri, mode="w")
    ds.create_dataset(compress_sample_dim=compress)

    skip_if_incompatible(array_uri)

    # Check for the presence of the Zstd filter
    found_zstd = False
    with tiledb.open(array_uri) as A:
        for filter in A.domain.dim("sample").filters:
            found_zstd = found_zstd or "Zstd" in str(filter)

    assert found_zstd == compress


@pytest.mark.parametrize("level", [1, 4, 16, 22])
def test_compression_level(tmp_path, level):
    # Create the dataset
    dataset_uri = os.path.join(tmp_path, "compression_level")
    array_uri = os.path.join(dataset_uri, "data")
    ds = tiledbvcf.Dataset(dataset_uri, mode="w")
    ds.create_dataset(compression_level=level)

    skip_if_incompatible(array_uri)

    # Check for the expected compression level
    with tiledb.open(array_uri) as A:
        for i in range(A.schema.nattr):
            attr = A.schema.attr(i)
            for filter in attr.filters:
                if "Zstd" in str(filter):
                    assert filter.level == level


# Ok to skip is missing bcftools in Windows CI job
@pytest.mark.skipif(
    os.environ.get("CI") == "true"
    and platform.system() == "Windows"
    and shutil.which("bcftools") is None,
    reason="no bcftools",
)
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


def test_context_manager():
    ds1_uri = os.path.join(TESTS_INPUT_DIR, "arrays/v4/ingested_2samples")
    expected_count1 = 14
    ds2_uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/synth-array")
    expected_count2 = 19565

    # Test the context manager
    with tiledbvcf.Dataset(ds1_uri) as ds:
        assert ds.count() == expected_count1

    with tiledbvcf.Dataset(ds2_uri) as ds:
        assert ds.count() == expected_count2

    # Open the datasets outside the context manager
    ds1 = tiledbvcf.Dataset(ds1_uri)
    assert ds1.count() == expected_count1

    ds2 = tiledbvcf.Dataset(ds2_uri)
    assert ds2.count() == expected_count2

    # Check that an exception is raised when trying to access a closed dataset
    ds1.close()
    with pytest.raises(Exception):
        assert ds1.count() == expected_count1

    assert ds2.count() == expected_count2

    ds2.close()
    with pytest.raises(Exception):
        assert ds2.count() == expected_count2


def test_delete_dataset(tmp_path):
    uri = os.path.join(tmp_path, "delete_dataset")

    with tiledbvcf.Dataset(uri, mode="w") as ds:
        ds.create_dataset()

    # Check that the dataset exists
    assert os.path.exists(uri)

    # Delete the dataset
    tiledbvcf.Dataset.delete(uri)

    # Check that the dataset does not exist
    assert not os.path.exists(uri)


def test_equality_old_new_format():
    old_ds = tiledbvcf.Dataset(os.path.join(TESTS_INPUT_DIR, "arrays/old_format"))
    new_ds = tiledbvcf.Dataset(os.path.join(TESTS_INPUT_DIR, "arrays/new_format"))

    assert old_ds.count() == new_ds.count()
    assert old_ds.samples() == new_ds.samples()
    assert old_ds.read().equals(new_ds.read())
