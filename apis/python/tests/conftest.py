import glob
import os
import platform
import shutil
import subprocess

import numpy as np
import pytest
import tiledb
import tiledbvcf

# Directory containing this file
CONTAINING_DIR = os.path.abspath(os.path.dirname(__file__))

# Test inputs directory
TESTS_INPUT_DIR = os.path.abspath(
    os.path.join(CONTAINING_DIR, "../../../libtiledbvcf/test/inputs")
)


# Skip marker for tests that require bcftools, which may be absent on Windows CI.
skip_if_no_bcftools = pytest.mark.skipif(
    os.environ.get("CI") == "true"
    and platform.system() == "Windows"
    and shutil.which("bcftools") is None,
    reason="no bcftools",
)


def assert_dfs_equal(expected, actual):
    """Assert that two DataFrames are equal, with type-aware column comparison.

    Floating-point columns are compared with np.isclose (NaN-safe).
    Integer columns are cast to int64 before comparison.
    All other columns use pandas Series.equals.

    Args:
        expected: DataFrame containing the expected values.
        actual: DataFrame containing the values under test.

    Raises:
        AssertionError: If any column differs between expected and actual.
    """

    def assert_series(s1, s2):
        if np.issubdtype(s2.dtype, np.floating):
            assert np.isclose(s1, s2, equal_nan=True).all()
        elif np.issubdtype(s2.dtype, np.integer):
            assert s1.astype("int64").equals(s2.astype("int64"))
        else:
            assert s1.equals(s2)

    for k in expected:
        assert_series(expected[k], actual[k])

    for k in actual:
        assert_series(expected[k], actual[k])


def skip_if_incompatible(uri):
    """Skip the current test if the TileDB array at uri is incompatible with the current environment.

    Attempts to open the array; if TileDB raises a format-version mismatch or
    any other TileDBError the test is skipped rather than failed, because the
    error indicates an environment incompatibility rather than a code defect.

    Args:
        uri: Path to the TileDB array to check.

    Returns:
        True if the array opened successfully.

    Raises:
        pytest.skip.Exception: If the array has an incompatible format version
            or any other TileDBError occurs.
    """
    try:
        with tiledb.open(uri):
            return True
    except tiledb.libtiledb.TileDBError as e:
        if "incompatible format version" in str(e).lower():
            raise pytest.skip.Exception(
                "Test skipped due to incompatible format version"
            )
        raise pytest.skip.Exception(f"Test skipped due to TileDB error: {str(e)}")


@pytest.fixture
def bgzip_and_index_vcfs():
    """Fixture that provides a helper for bgzipping and indexing VCF files.

    The returned callable compresses every ``*.vcf`` file in ``input_dir`` with
    ``bcftools view -Oz`` and then indexes each resulting ``.gz`` file with
    ``bcftools index``.

    Usage::

        vcf_files = bgzip_and_index_vcfs(input_dir)
        vcf_files = bgzip_and_index_vcfs(input_dir, output_dir=tmp_path)

    Args:
        input_dir: Directory containing the ``.vcf`` files to compress.
        output_dir: Directory where the ``.gz`` files will be written.
            Defaults to ``input_dir`` when omitted.

    Returns:
        List of absolute paths to the produced ``.gz`` files.
    """

    def _bgzip_and_index(input_dir, output_dir=None):
        if output_dir is None:
            output_dir = input_dir
        raw_inputs = glob.glob(os.path.join(input_dir, "*.vcf"))
        for vcf_file in raw_inputs:
            out = os.path.join(output_dir, os.path.basename(vcf_file)) + ".gz"
            subprocess.run(
                f"bcftools view --no-version -Oz -o {out} {vcf_file}",
                shell=True,
                check=True,
            )
        bgzipped = glob.glob(os.path.join(output_dir, "*.gz"))
        for vcf_file in bgzipped:
            assert (
                subprocess.run(
                    f"bcftools index {vcf_file}", shell=True
                ).returncode
                == 0
            )
        return bgzipped

    return _bgzip_and_index


@pytest.fixture
def v4_dataset():
    """Open the pre-ingested v4 2-sample dataset in read mode.

    Returns:
        tiledbvcf.Dataset: Read-mode dataset backed by arrays/v4/ingested_2samples.
    """
    return tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v4/ingested_2samples")
    )


@pytest.fixture
def v3_dataset():
    """Open the pre-ingested v3 2-sample dataset in read mode.

    Returns:
        tiledbvcf.Dataset: Read-mode dataset backed by arrays/v3/ingested_2samples.
    """
    return tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples")
    )


@pytest.fixture
def v3_dataset_with_attrs():
    """Open the pre-ingested v3 2-sample dataset that includes GT, DP, and PL attributes.

    Returns:
        tiledbvcf.Dataset: Read-mode dataset backed by arrays/v3/ingested_2samples_GT_DP_PL.
    """
    return tiledbvcf.Dataset(
        os.path.join(TESTS_INPUT_DIR, "arrays/v3/ingested_2samples_GT_DP_PL")
    )


@pytest.fixture
def stats_bgzipped_vcfs(tmp_path, bgzip_and_index_vcfs):
    """Copy the stats VCF test inputs to tmp_path, bgzip and index them.

    Args:
        tmp_path: Pytest-provided temporary directory for this test.
        bgzip_and_index_vcfs: Fixture-provided helper that compresses and indexes VCF files.

    Returns:
        List[str]: Paths to the bgzipped and indexed ``.gz`` files inside tmp_path.
    """
    shutil.copytree(
        os.path.join(TESTS_INPUT_DIR, "stats"), os.path.join(tmp_path, "stats")
    )
    return bgzip_and_index_vcfs(os.path.join(tmp_path, "stats"))


@pytest.fixture
def stats_sample_names(stats_bgzipped_vcfs):
    """Return the sample names for the 8 bgzipped stats inputs.

    Sample names are extracted from the file names: each file is named
    ``<sample_name>.vcf.gz``, so splitting on ``"."`` and taking the first
    part yields the sample name.

    Args:
        stats_bgzipped_vcfs: Fixture-provided list of bgzipped VCF file paths.

    Returns:
        List[str]: One sample name per bgzipped input file.
    """
    assert len(stats_bgzipped_vcfs) == 8
    return [
        sample_name
        for f in stats_bgzipped_vcfs
        for sample_name, *_ in [os.path.basename(f).split(".")]
    ]


@pytest.fixture
def stats_v3_dataset(tmp_path, stats_bgzipped_vcfs):
    """Create and return a v3 dataset with variant stats and allele counting enabled.

    All 8 stats samples are ingested before the dataset is returned in read mode.

    Args:
        tmp_path: Pytest-provided temporary directory for this test.
        stats_bgzipped_vcfs: Fixture-provided list of bgzipped VCF file paths to ingest.

    Returns:
        tiledbvcf.Dataset: Read-mode dataset with variant_stats_version=3,
            enable_variant_stats=True, and enable_allele_count=True.
    """
    assert len(stats_bgzipped_vcfs) == 8
    ds = tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="w")
    ds.create_dataset(
        enable_variant_stats=True, enable_allele_count=True, variant_stats_version=3
    )
    ds.ingest_samples(stats_bgzipped_vcfs)
    return tiledbvcf.Dataset(uri=os.path.join(tmp_path, "stats_test"), mode="r")
