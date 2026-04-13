import os
import platform
import shutil

import pandas as pd
import pyarrow as pa
import pytest
import tiledbvcf

from .conftest import skip_if_no_bcftools, TESTS_INPUT_DIR, assert_dfs_equal

@skip_if_no_bcftools
def test_read_with_af_filter(stats_v3_dataset, stats_sample_names):
    """Verify that set_af_filter restricts results by allele frequency for both pandas and Arrow."""
    attrs = ["contig", "pos_start", "id", "qual", "info_TILEDB_IAF", "sample_name"]
    df = stats_v3_dataset.read(
        samples=stats_sample_names,
        attrs=attrs,
        set_af_filter="<0.2",
    )
    assert df.shape == (1, 8)
    assert df.query("sample_name == 'second'")["qual"].iloc[0] == pytest.approx(343.73)
    assert df[df["sample_name"] == "second"]["info_TILEDB_IAF"].iloc[0][0] == 0.9375

    tbl = stats_v3_dataset.read_arrow(
        samples=stats_sample_names,
        attrs=attrs,
        set_af_filter="<0.2",
    )
    assert tbl.num_rows == 1
    assert tbl.to_pandas().equals(df)


@skip_if_no_bcftools
def test_read_with_scan_all_samples(stats_v3_dataset, stats_sample_names):
    """Verify scan_all_samples normalizes IAF across all samples for both pandas and Arrow."""
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

    tbl = stats_v3_dataset.read_arrow(
        samples=stats_sample_names,
        attrs=attrs,
        scan_all_samples=True,
    )
    assert tbl.num_rows == len(df)
    assert tbl.to_pandas().equals(df)


@skip_if_no_bcftools
def test_read_with_af_filter_and_scan_all_samples(stats_v3_dataset, stats_sample_names):
    """Verify set_af_filter and scan_all_samples can be combined to widen the result set."""
    attrs = ["contig", "pos_start", "sample_name"]

    df_filter_only = stats_v3_dataset.read(
        samples=stats_sample_names,
        attrs=attrs,
        set_af_filter="<0.2",
    )

    df = stats_v3_dataset.read(
        samples=stats_sample_names,
        attrs=attrs,
        set_af_filter="<0.2",
        scan_all_samples=True,
    )
    assert len(df) > len(df_filter_only)

    tbl = stats_v3_dataset.read_arrow(
        samples=stats_sample_names,
        attrs=attrs,
        set_af_filter="<0.2",
        scan_all_samples=True,
    )
    assert tbl.to_pandas().equals(df)


@skip_if_no_bcftools
def test_variant_stats_parameter_errors(stats_v3_dataset):
    """Verify that read_variant_stats and read_variant_stats_arrow reject invalid parameters."""
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


@skip_if_no_bcftools
def test_variant_stats_empty_region(stats_v3_dataset):
    """Verify read_variant_stats returns an empty DataFrame for a region with no variants."""
    assert stats_v3_dataset.read_variant_stats(regions=["chr3:1-10000"]).empty


@skip_if_no_bcftools
def test_variant_stats_return_types(stats_v3_dataset):
    """Verify read_variant_stats returns a DataFrame and read_variant_stats_arrow returns an Arrow Table."""
    # Both the deprecated positional `region` parameter and the `regions` list
    # should return a DataFrame / Arrow Table of the same shape and content.
    region = "chr1:1-10000"
    with pytest.warns(DeprecationWarning, match='"region" parameter is deprecated'):
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


@skip_if_no_bcftools
def test_variant_stats_multi_contig_regions(stats_v3_dataset):
    """Verify read_variant_stats handles multiple contig regions and sorts results by contig."""
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


@skip_if_no_bcftools
def test_variant_stats_overlapping_regions(stats_v3_dataset):
    """Verify read_variant_stats deduplicates and merges overlapping regions on the same contig."""
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


@skip_if_no_bcftools
def test_variant_stats_scan_all_samples(stats_v3_dataset):
    """Verify scan_all_samples normalizes allele number (an) across all samples in variant stats."""
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


@skip_if_no_bcftools
def test_variant_stats_drop_ref(stats_v3_dataset):
    """Verify drop_ref=True filters out reference allele rows from variant stats."""
    # drop_ref=True filters out rows where the alternate allele is "ref".
    regions = ["chr2:1-1", "chr2:3-3", "chr1:1-1", "chr1:1-2", "chr1:3-4", "chr1:2-5"]

    df = stats_v3_dataset.read_variant_stats(regions=regions)
    assert ["T,C", "ref", "G,GTTTA", "G,T", "ref", "C,A", "C,G", "C,T", "ref",
            "G,GTTTA", "ref", "C,T", "ref", "G,GTTTA", "G,GTTTA"] == list(df["alleles"].values)

    df = stats_v3_dataset.read_variant_stats(regions=regions, drop_ref=True)
    assert ["T,C", "G,GTTTA", "G,T", "C,A", "C,G", "C,T", "G,GTTTA",
            "C,T", "G,GTTTA", "G,GTTTA"] == list(df["alleles"].values)


@skip_if_no_bcftools
def test_allele_count_parameter_errors(stats_v3_dataset):
    """Verify that read_allele_count and read_allele_count_arrow reject invalid parameters."""
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


@skip_if_no_bcftools
def test_allele_count_empty_region(stats_v3_dataset):
    """Verify read_allele_count returns an empty DataFrame for a region with no data."""
    assert stats_v3_dataset.read_allele_count(regions=["chr3:1-10000"]).empty


@skip_if_no_bcftools
def test_allele_count_return_types(stats_v3_dataset):
    """Verify read_allele_count returns a DataFrame and read_allele_count_arrow returns an Arrow Table."""
    # Both the deprecated positional `region` parameter and the `regions` list
    # should return a DataFrame / Arrow Table of the same shape and content.
    region = "chr1:1-10000"
    expected_pos = (0, 1, 1, 2, 2, 2, 3)
    expected_count = (8, 5, 3, 4, 2, 2, 1)

    with pytest.warns(DeprecationWarning, match='"region" parameter is deprecated'):
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


@skip_if_no_bcftools
def test_allele_count_multi_contig_regions(stats_v3_dataset):
    """Verify read_allele_count handles multiple contig regions and sorts results by contig."""
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


@skip_if_no_bcftools
def test_allele_count_overlapping_regions(stats_v3_dataset):
    """Verify read_allele_count deduplicates and merges overlapping regions on the same contig."""
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


@skip_if_no_bcftools
def test_allele_frequency(stats_v3_dataset, tmp_path):
    """Verify allele frequency consistency: ac / af rounds to an."""
    # Verify that ac / af ≈ an (i.e. allele frequency is consistent with counts).
    region = "chr1:1-10000"
    # read_allele_frequency internally uses the deprecated `region` parameter.
    with pytest.warns(DeprecationWarning, match='"region" parameter is deprecated'):
        df = tiledbvcf.allele_frequency.read_allele_frequency(
            os.path.join(tmp_path, "stats_test"), region
        )
    assert df.pos.is_monotonic_increasing
    df["an_check"] = (df.ac / df.af).round(0).astype("int32")
    assert df.an_check.equals(df.an)
    assert stats_v3_dataset.read_variant_stats(regions=[region]).shape == (13, 6)


@skip_if_no_bcftools
def test_allele_frequency_invalid_region_format(stats_v3_dataset, tmp_path):
    """Verify read_allele_frequency rejects a badly-formatted region string."""
    uri = os.path.join(tmp_path, "stats_test")
    with pytest.warns(DeprecationWarning, match='"region" parameter is deprecated'):
        with pytest.raises(Exception, match='"region" parameter must have format'):
            tiledbvcf.allele_frequency.read_allele_frequency(uri, "chr1")


@skip_if_no_bcftools
def test_allele_frequency_empty_region(stats_v3_dataset, tmp_path):
    """Verify read_allele_frequency returns an empty DataFrame for a region with no data."""
    uri = os.path.join(tmp_path, "stats_test")
    with pytest.warns(DeprecationWarning, match='"region" parameter is deprecated'):
        df = tiledbvcf.allele_frequency.read_allele_frequency(uri, "chr3:1-10000")
    assert df.empty


def test_sample_qc_samples_parameter(tmp_path):
    """Verify sample_qc can be filtered to specific samples."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(enable_variant_stats=True, enable_allele_count=True)
    ds.ingest_samples(
        [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small3.bcf"]]
    )

    qc_all = tiledbvcf.sample_qc(uri)
    assert set(qc_all["sample"]) == {"HG00280", "HG01762"}

    qc_one = tiledbvcf.sample_qc(uri, samples=["HG00280"])
    assert list(qc_one["sample"]) == ["HG00280"]
    assert len(qc_one) == 1


def test_sample_qc_config_parameter(tmp_path):
    """Smoke Test: Verify sample_qc accepts a config parameter."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(enable_variant_stats=True, enable_allele_count=True)
    ds.ingest_samples(
        [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small3.bcf"]]
    )

    qc_default = tiledbvcf.sample_qc(uri)
    qc_with_config = tiledbvcf.sample_qc(uri, config={"sm.tile_cache_size": "0"})
    assert_dfs_equal(qc_default, qc_with_config)
