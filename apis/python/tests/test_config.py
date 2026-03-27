import os

import pytest
import tiledb
import tiledbvcf

from .conftest import TESTS_INPUT_DIR

@pytest.mark.parametrize("level", ["fatal", "error", "warn", "info", "debug", "trace"])
def test_config_logging_valid_levels(level):
    """Smoke Test: Verify all documented log levels are accepted."""
    tiledbvcf.config_logging(level)


def test_config_logging_invalid_level_raises():
    """Verify an unrecognized log level raises an exception."""
    with pytest.raises(Exception, match="Unsupported log level"):
        tiledbvcf.config_logging("verbose")


def test_config_logging_log_file(tmp_path):
    """Smoke Test: Verify a log_file path is accepted."""
    tiledbvcf.config_logging("fatal", log_file=str(tmp_path / "tiledbvcf.log"))


def test_read_config():
    """Verify that ReadConfig parameters are accepted and that invalid parameters raise."""
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


def test_read_limit():
    """Verify that ReadConfig limit truncates results to the specified number of rows."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(limit=3)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(
        attrs=["sample_name", "pos_start", "pos_end", "fmt_DP", "fmt_PL"],
        regions=["1:12100-13360", "1:13500-17350"],
    )
    assert len(df) == 3


def test_region_partitioned_read():
    """Verify that region_partition splits reads across partitions correctly."""
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
    """Verify that sample_partition splits reads by sample correctly."""
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
    """Verify combined sample and region partitioning."""
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
    """Verify disabling region sorting returns the same records as sorted."""
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
    """Smoke Test: Verify non-default buffer and tile cache percentages are accepted."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(buffer_percentage=30, tiledb_tile_cache_percentage=5)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(attrs=["sample_name", "pos_start"], regions=["1:12000-13000"])
    assert len(df) > 0


def test_tiledb_config_as_dict():
    """Smoke Test: Verify tiledb_config accepts a dict."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    cfg = tiledbvcf.ReadConfig(
        tiledb_config={"sm.tile_cache_size": "0", "sm.compute_concurrency_level": "1"}
    )
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(attrs=["sample_name", "pos_start"], regions=["1:12000-13000"])
    assert len(df) > 0


def test_tiledb_config_as_tiledb_config_object():
    """Smoke Test: Verify tiledb_config accepts a tiledb.Config object."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    tiledb_cfg = tiledb.Config({"sm.tile_cache_size": "0"})
    cfg = tiledbvcf.ReadConfig(tiledb_config=tiledb_cfg)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    df = ds.read(attrs=["sample_name", "pos_start"], regions=["1:12000-13000"])
    assert len(df) > 0


@pytest.mark.skipif(os.environ.get("CI") != "true", reason="CI only")
def test_large_export_correctness():
    """Verify large export from S3 produces the expected total and unique record counts."""
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

