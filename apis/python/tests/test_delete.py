import os

import pytest
import tiledb
import tiledbvcf

from .conftest import skip_if_no_bcftools, TESTS_INPUT_DIR


def test_delete_dataset(tmp_path):
    """Verify that Dataset.delete() removes the dataset from disk."""
    uri = os.path.join(tmp_path, "delete_dataset")

    with tiledbvcf.Dataset(uri, mode="w") as ds:
        ds.create_dataset()

    assert os.path.exists(uri)
    tiledbvcf.Dataset.delete(uri)
    assert not os.path.exists(uri)


def test_delete_dataset_with_config(tmp_path):
    """Smoke Test: Verify Dataset.delete() accepts a config parameter."""
    uri = os.path.join(tmp_path, "delete_dataset")

    with tiledbvcf.Dataset(uri, mode="w") as ds:
        ds.create_dataset()

    assert os.path.exists(uri)
    tiledbvcf.Dataset.delete(uri, config={"sm.tile_cache_size": "0"})
    assert not os.path.exists(uri)


def test_delete_dataset_nonexistent_uri_raises(tmp_path):
    """Verify deleting a nonexistent URI raises TileDBError."""
    uri = os.path.join(tmp_path, "nonexistent")
    with pytest.raises(tiledb.TileDBError):
        tiledbvcf.Dataset.delete(uri)


@skip_if_no_bcftools
def test_delete_samples(tmp_path, stats_v3_dataset, stats_sample_names):
    """Verify that delete_samples() removes the specified samples from the dataset."""
    assert "second" in stats_sample_names
    assert "fifth" in stats_sample_names
    assert "third" in stats_sample_names
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="w")
    ds.delete_samples(["second", "fifth"])
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="r")
    sample_names = ds.samples()
    assert "second" not in sample_names
    assert "fifth" not in sample_names
    assert "third" in sample_names


def test_delete_samples_empty_list_is_noop(tmp_path):
    """Verify delete_samples with an empty list is a no-op."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small2.bcf"]])

    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.delete_samples([])

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert set(ds.samples()) == {"HG00280", "HG01762"}


def test_delete_samples_none_raises(tmp_path):
    """Verify delete_samples(None) raises TypeError."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()

    with pytest.raises(TypeError):
        ds.delete_samples(None)


def test_delete_samples_nonexistent_raises(tmp_path):
    """Verify deleting a nonexistent sample raises RuntimeError."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="w")
    with pytest.raises(RuntimeError, match="Sample not found in dataset"):
        ds.delete_samples(["NONEXISTENT"])


@skip_if_no_bcftools
def test_delete_samples_skip_aggregate_stats(tmp_path, stats_v3_dataset):
    """Verify skip_aggregate_stats skips stats update and records metadata."""
    uri = os.path.join(tmp_path, "stats_test")
    ds = tiledbvcf.Dataset(uri=uri, mode="w")
    ds.delete_samples(["second"], skip_aggregate_stats=True)

    # Verify sample is deleted
    ds = tiledbvcf.Dataset(uri=uri, mode="r")
    sample_names = ds.samples()
    assert "second" not in sample_names
    assert "third" in sample_names

    # Verify skipped_delete_samples metadata on allele_count and variant_stats
    for array_name in ["allele_count", "variant_stats"]:
        with tiledb.open(os.path.join(uri, array_name), "r") as arr:
            meta = arr.meta["skipped_delete_samples"]
            assert b"second" in meta


@skip_if_no_bcftools
def test_delete_samples_skip_aggregate_stats_accumulates(
    tmp_path, stats_v3_dataset
):
    """Verify skipped_delete_samples metadata accumulates across deletions."""
    uri = os.path.join(tmp_path, "stats_test")

    ds = tiledbvcf.Dataset(uri=uri, mode="w")
    ds.delete_samples(["second"], skip_aggregate_stats=True)

    ds = tiledbvcf.Dataset(uri=uri, mode="w")
    ds.delete_samples(["fifth"], skip_aggregate_stats=True)

    for array_name in ["allele_count", "variant_stats"]:
        with tiledb.open(os.path.join(uri, array_name), "r") as arr:
            meta = arr.meta["skipped_delete_samples"]
            assert meta == b"second,fifth"


def test_delete_samples_read_mode_raises(tmp_path):
    """Verify delete_samples() raises in read mode."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="r")
    with pytest.raises(Exception, match="Dataset not open in write mode"):
        ds.delete_samples(["HG00280"])
