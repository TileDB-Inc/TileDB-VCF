import os

import pytest
import tiledb
import tiledbvcf

from .conftest import TESTS_INPUT_DIR

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


def test_sort_regions():
    """sort_regions=False is accepted and returns the same records as sort_regions=True."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    regions = ["1:17000-18000", "1:12000-13000"]  # intentionally out of order

    cfg_sorted = tiledbvcf.ReadConfig(sort_regions=True)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg_sorted)
    df_sorted = ds.read(attrs=["sample_name", "pos_start"], regions=regions)

    cfg_unsorted = tiledbvcf.ReadConfig(sort_regions=False)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg_unsorted)
    df_unsorted = ds.read(attrs=["sample_name", "pos_start"], regions=regions)

    assert len(df_sorted) == len(df_unsorted)


def test_buffer_percentage_and_tile_cache_percentage():
    """Non-default buffer_percentage and tiledb_tile_cache_percentage are accepted."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(buffer_percentage=30, tiledb_tile_cache_percentage=5)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(attrs=["sample_name", "pos_start"], regions=["1:12000-13000"])
    assert len(df) > 0


def test_tiledb_config_as_dict():
    """tiledb_config can be supplied as a dict instead of a list of strings."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(
        tiledb_config={"sm.tile_cache_size": "0", "sm.compute_concurrency_level": "1"}
    )
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(attrs=["sample_name", "pos_start"], regions=["1:12000-13000"])
    assert len(df) > 0


def test_tiledb_config_as_tiledb_config_object():
    """tiledb_config can be supplied as a tiledb.Config object."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    tiledb_cfg = tiledb.Config({"sm.tile_cache_size": "0"})
    cfg = tiledbvcf.ReadConfig(tiledb_config=tiledb_cfg)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(attrs=["sample_name", "pos_start"], regions=["1:12000-13000"])
    assert len(df) > 0


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

