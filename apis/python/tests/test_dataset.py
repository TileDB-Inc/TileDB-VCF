import os

import pytest
import tiledbvcf

from .conftest import TESTS_INPUT_DIR


def test_invalid_mode_raises():
    """An unrecognised mode string raises at construction time."""
    uri = os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    with pytest.raises(Exception, match="Unsupported dataset mode"):
        tiledbvcf.Dataset(uri, mode="x")


def test_version(v3_dataset, v4_dataset):
    """version() returns a multi-line string containing TileDB-VCF, TileDB, and htslib versions."""
    for ds in [v3_dataset, v4_dataset]:
        v = ds.version()
        assert "TileDB-VCF version" in v
        assert "TileDB version" in v
        assert "htslib version" in v


def test_schema_version(v3_dataset, v4_dataset):
    """schema_version() returns the correct integer schema version for each dataset."""
    assert v3_dataset.schema_version() == 3
    assert v4_dataset.schema_version() == 4


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
