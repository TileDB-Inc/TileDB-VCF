import numpy as np
import os
import pandas as pd
import pytest
import tiledbvcf

# Directory containing this file
CONTAINING_DIR = os.path.abspath(os.path.dirname(__file__))

# Test inputs directory
TESTS_INPUT_DIR = os.path.abspath(
    os.path.join(CONTAINING_DIR, "../../../libtiledbvcf/test/inputs")
)


def _check_dfs(expected, actual):
    def assert_series(s1, s2):
        if type(s2.iloc[0]) == np.ndarray:
            assert len(s1) == len(s2)
            for i in range(0, len(s1)):
                assert np.array_equal(s1.iloc[i], s2.iloc[i])
        else:
            assert s1.equals(s2)

    for k in expected:
        assert_series(expected[k], actual[k])

    for k in actual:
        assert_series(expected[k], actual[k])


@pytest.fixture
def test_ds():
    return tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    )


@pytest.fixture
def test_ds_attrs():
    return tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples_GT_DP_PL")
    )


def test_basic_count(test_ds):
    assert test_ds.count() == 14


def test_read_must_specify_attrs(test_ds):
    with pytest.raises(Exception):
        df = test_ds.read()


def test_retrieve_attributes(test_ds):
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
    assert sorted(test_ds.attributes(attr_type="builtin")) == sorted(builtin_attrs)

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
    assert test_ds.attributes(attr_type="info") == info_attrs

    fmt_attrs = [
        "fmt_AD",
        "fmt_DP",
        "fmt_GQ",
        "fmt_GT",
        "fmt_MIN_DP",
        "fmt_PL",
        "fmt_SB",
    ]
    assert test_ds.attributes(attr_type="fmt") == fmt_attrs


def test_retrieve_samples(test_ds):
    assert test_ds.samples() == ["HG00280", "HG01762"]


def test_read_attrs(test_ds_attrs):
    attrs = ["sample_name"]
    df = test_ds_attrs.read(attrs=attrs)
    assert df.columns.values.tolist() == attrs

    attrs = ["sample_name", "fmt_GT"]
    df = test_ds_attrs.read(attrs=attrs)
    assert df.columns.values.tolist() == attrs

    attrs = ["sample_name"]
    df = test_ds_attrs.read(attrs=attrs)
    assert df.columns.values.tolist() == attrs


def test_basic_reads(test_ds):
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

    for use_arrow in [False, True]:
        func = test_ds.read_arrow if use_arrow else test_ds.read

        df = func(attrs=["sample_name", "pos_start", "pos_end"])
        if use_arrow:
            df = df.to_pandas()

        _check_dfs(
            expected_df,
            df.sort_values(ignore_index=True, by=["sample_name", "pos_start"]),
        )

    # Region intersection
    df = test_ds.read(
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
    _check_dfs(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )

    # Region and sample intersection
    df = test_ds.read(
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
    _check_dfs(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )

    # Sample only
    df = test_ds.read(
        attrs=["sample_name", "pos_start", "pos_end"], samples=["HG01762"]
    )
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(["HG01762", "HG01762", "HG01762"]),
            "pos_start": pd.Series([12141, 12546, 13354], dtype=np.int32),
            "pos_end": pd.Series([12277, 12771, 13389], dtype=np.int32),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    _check_dfs(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_multiple_counts(test_ds):
    assert test_ds.count() == 14
    assert test_ds.count() == 14
    assert test_ds.count(regions=["1:12700-13400"]) == 6
    assert test_ds.count(samples=["HG00280"], regions=["1:12700-13400"]) == 4
    assert test_ds.count() == 14
    assert test_ds.count(samples=["HG01762"]) == 3
    assert test_ds.count(samples=["HG00280"]) == 11


def test_empty_region(test_ds):
    assert test_ds.count(regions=["12:1-1000000"]) == 0


def test_missing_sample_raises_exception(test_ds):
    with pytest.raises(RuntimeError):
        test_ds.count(samples=["abcde"])


# TODO remove skip
@pytest.mark.skip
def test_bad_contig_raises_exception(test_ds):
    with pytest.raises(RuntimeError):
        test_ds.count(regions=["chr1:1-1000000"])
    with pytest.raises(RuntimeError):
        test_ds.count(regions=["1"])
    with pytest.raises(RuntimeError):
        test_ds.count(regions=["1:100-"])
    with pytest.raises(RuntimeError):
        test_ds.count(regions=["1:-100"])


def test_bad_attr_raises_exception(test_ds):
    with pytest.raises(RuntimeError):
        test_ds.read(attrs=["abcde"], regions=["1:12700-13400"])


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
    test_ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    df = test_ds.read(attrs=["pos_end"], regions=["1:12700-13400"])
    assert not test_ds.read_completed()
    assert len(df) == 2
    _check_dfs(
        pd.DataFrame.from_dict({"pos_end": np.array([12771, 12771], dtype=np.int32)}),
        df,
    )

    df = test_ds.continue_read()
    assert not test_ds.read_completed()
    assert len(df) == 2
    _check_dfs(
        pd.DataFrame.from_dict({"pos_end": np.array([13374, 13389], dtype=np.int32)}),
        df,
    )

    df = test_ds.continue_read()
    assert test_ds.read_completed()
    assert len(df) == 2
    _check_dfs(
        pd.DataFrame.from_dict({"pos_end": np.array([13395, 13413], dtype=np.int32)}),
        df,
    )

    # test incomplete via read_arrow
    table = test_ds.read_arrow(attrs=["pos_end"], regions=["1:12700-13400"])
    assert not test_ds.read_completed()
    assert len(table) == 2
    _check_dfs(
        pd.DataFrame.from_dict({"pos_end": np.array([12771, 12771], dtype=np.int32)}),
        table.to_pandas(),
    )

    table = test_ds.continue_read_arrow()
    assert not test_ds.read_completed()
    assert len(table) == 2
    _check_dfs(
        pd.DataFrame.from_dict({"pos_end": np.array([13374, 13389], dtype=np.int32)}),
        table.to_pandas(),
    )

    table = test_ds.continue_read_arrow()
    assert test_ds.read_completed()
    assert len(table) == 2
    _check_dfs(
        pd.DataFrame.from_dict({"pos_end": np.array([13395, 13413], dtype=np.int32)}),
        table.to_pandas(),
    )


def test_incomplete_read_generator():
    # Using undocumented "0 MB" budget to test incomplete reads.
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=0)
    test_ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    overall_df = None
    for df in test_ds.read_iter(attrs=["pos_end"], regions=["1:12700-13400"]):
        if overall_df is None:
            overall_df = df
        else:
            overall_df = overall_df.append(df, ignore_index=True)

    assert len(overall_df) == 6
    _check_dfs(
        pd.DataFrame.from_dict(
            {
                "pos_end": np.array(
                    [12771, 12771, 13374, 13389, 13395, 13413], dtype=np.int32
                )
            }
        ),
        overall_df,
    )


def test_read_filters(test_ds):
    df = test_ds.read(
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
                    lambda lst: np.array(lst, dtype=np.object),
                    [None, None, ["LowQual"], None, None, None],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    _check_dfs(
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
                    lambda lst: np.array(lst, dtype=np.object),
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

    _check_dfs(expected_df, df.sort_values(ignore_index=True, by=["pos_start"]))


def test_read_alleles(test_ds):
    df = test_ds.read(
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
                    lambda lst: np.array(lst, dtype=np.object),
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
    _check_dfs(
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
                    lambda lst: np.array(lst, dtype=np.object),
                    [["T", "CCCCTCCCT", "C", "CCCCTCCCTCCCT", "CCCCT"], ["CTG", "C"]],
                )
            ),
            "id": pd.Series([".", "rs1497816"]),
            "filters": pd.Series(
                map(
                    lambda lst: np.array(lst, dtype=np.object),
                    [["LowQual"], ["LowQual"]],
                )
            ),
        }
    ).sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    _check_dfs(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_read_var_len_attrs(test_ds):
    df = test_ds.read(
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

    _check_dfs(
        expected_df, df.sort_values(ignore_index=True, by=["sample_name", "pos_start"])
    )


def test_sample_args(test_ds, tmp_path):
    sample_file = os.path.join(tmp_path, "1_sample.txt")
    with open(sample_file, "w") as file:
        file.write("HG00280")

    region = ["1:12141-12141"]
    df1 = test_ds.read(["sample_name"], regions=region, samples=["HG00280"])
    df2 = test_ds.read(["sample_name"], regions=region, samples_file=sample_file)
    _check_dfs(df1, df2)

    with pytest.raises(TypeError):
        test_ds.read(
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
    _check_dfs(
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


def test_large_export_correctness():
    uri = "s3://tiledb-inc-demo-data/tiledbvcf-arrays/v4/vcf-samples-20"

    ds = tiledbvcf.Dataset(uri, mode="r", verbose=True)
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
    ds2 = tiledbvcf.Dataset(uri, mode="w", verbose=True)
    samples = [
        os.path.join(TESTS_INPUT_DIR, s) for s in ["v2-DjrIAzkP-downsampled.vcf.gz"]
    ]
    ds2.create_dataset()
    ds2.ingest_samples(samples, contigs_to_keep_separate=["chr1"])

    # Open it back in read mode and check some queries
    ds2 = tiledbvcf.Dataset(uri, cfg=cfg, mode="r", verbose=True)
    df2 = ds2.read(attrs=attrs)
    print(df.equals(df2))
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
