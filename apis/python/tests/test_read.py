import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
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
