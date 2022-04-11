import numpy as np
import os
import pandas as pd
import pytest
import tiledbvcf

import dask
import dask.distributed

# Directory containing this file
CONTAINING_DIR = os.path.abspath(os.path.dirname(__file__))

# Test inputs directory
TESTS_INPUT_DIR = os.path.abspath(
    os.path.join(CONTAINING_DIR, "../../../libtiledbvcf/test/inputs")
)

dask_cluster = dask.distributed.LocalCluster(
    n_workers=2, threads_per_worker=1, memory_limit="4GB", processes=False
)
dask_client = dask.distributed.Client(dask_cluster)


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
    )

    # Region partitions
    dask_df = test_ds.read_dask(
        attrs=["sample_name", "pos_start", "pos_end"], region_partitions=10
    )
    df = dask_df.compute()
    _check_dfs(expected_df, df)

    # Sample partitions (we have to sort to check the result)
    dask_df = test_ds.read_dask(
        attrs=["sample_name", "pos_start", "pos_end"], sample_partitions=2
    )
    df = dask_df.compute().sort_values("sample_name").reset_index(drop=True)
    _check_dfs(expected_df.sort_values("sample_name").reset_index(drop=True), df)

    # Both
    dask_df = test_ds.read_dask(
        attrs=["sample_name", "pos_start", "pos_end"],
        region_partitions=10,
        sample_partitions=2,
    )
    df = dask_df.compute().sort_values("sample_name").reset_index(drop=True)
    _check_dfs(expected_df.sort_values("sample_name").reset_index(drop=True), df)

    # No partitioning
    dask_df = test_ds.read_dask(attrs=["sample_name", "pos_start", "pos_end"])
    df = dask_df.compute()
    _check_dfs(expected_df, df)


def test_incomplete_reads():
    # Using undocumented "0 MB" budget to test incomplete reads.
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=0)
    test_ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

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
    )

    # Region partitions
    dask_df = test_ds.read_dask(  # pylint:disable=no-member
        attrs=["sample_name", "pos_start", "pos_end"], region_partitions=10
    )  # pylint:disable=no-member
    df = dask_df.compute()
    _check_dfs(expected_df, df)

    # Sample partitions (we have to sort to check the result)
    dask_df = test_ds.read_dask(  # pylint:disable=no-member
        attrs=["sample_name", "pos_start", "pos_end"], sample_partitions=2
    )
    df = dask_df.compute().sort_values("sample_name").reset_index(drop=True)
    _check_dfs(expected_df.sort_values("sample_name").reset_index(drop=True), df)

    # Both
    dask_df = test_ds.read_dask(  # pylint:disable=no-member
        attrs=["sample_name", "pos_start", "pos_end"],
        region_partitions=10,
        sample_partitions=2,
    )
    df = dask_df.compute().sort_values("sample_name").reset_index(drop=True)
    _check_dfs(expected_df.sort_values("sample_name").reset_index(drop=True), df)

    # No partitioning
    dask_df = test_ds.read_dask(  # pylint:disable=no-member
        attrs=["sample_name", "pos_start", "pos_end"]
    )
    df = dask_df.compute()
    _check_dfs(expected_df, df)

    # Subset of partitions (limit_partitions)
    dask_df = test_ds.read_dask(  # pylint:disable=no-member
        attrs=["sample_name", "pos_start", "pos_end"],
        region_partitions=10,
        sample_partitions=2,
        limit_partitions=2,
    )
    assert dask_df.npartitions == 2


def test_basic_map(test_ds):
    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(["HG00280", "HG01762"]),
            "pos_start": pd.Series([12141, 12141], dtype=np.int32),
            "pos_end": pd.Series([12277, 12277], dtype=np.int32),
        }
    )

    # Region partitions
    dask_df = test_ds.map_dask(
        lambda df: df[df.pos_start * 2 < 25000],
        attrs=["sample_name", "pos_start", "pos_end"],
        region_partitions=10,
    )  # pylint:disable=no-member
    df = dask_df.compute()
    _check_dfs(expected_df, df)


def test_map_incomplete():
    # Using undocumented "0 MB" budget to test incomplete reads.
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=0)
    test_ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

    expected_df = pd.DataFrame(
        {
            "sample_name": pd.Series(["HG00280", "HG01762"]),
            "pos_start": pd.Series([12141, 12141], dtype=np.int32),
            "pos_end": pd.Series([12277, 12277], dtype=np.int32),
        }
    )

    # Region partitions
    dask_df = test_ds.map_dask(  # pylint:disable=no-member
        lambda df: df[df.pos_start * 2 < 25000],
        attrs=["sample_name", "pos_start", "pos_end"],
        region_partitions=10,
    )  # pylint:disable=no-member
    df = dask_df.compute()
    _check_dfs(expected_df, df)
