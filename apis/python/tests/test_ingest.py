import os
import platform
import shutil

import numpy as np
import pandas as pd
import pytest
import tiledb
import tiledbvcf

from .conftest import (
    assert_dfs_equal,
    skip_if_incompatible,
    skip_if_no_bcftools,
    TESTS_INPUT_DIR,
)

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


@skip_if_no_bcftools
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
    df = ds.read_variant_stats(regions=["chr1:1-10000"])
    assert df.shape == (13, 6)
    # read_allele_frequency internally uses the deprecated `region` parameter.
    with pytest.warns(DeprecationWarning, match='"region" parameter is deprecated'):
        df = tiledbvcf.allele_frequency.read_allele_frequency(
            os.path.join(tmp_path, "stats_test"), "chr1:1-10000"
        )
    assert df.pos.is_monotonic_increasing
    df["an_check"] = (df.ac / df.af).round(0).astype("int32")
    assert df.an_check.equals(df.an)
    df = ds.read_variant_stats(regions=["chr1:1-10000"])
    assert df.shape == (13, 6)
    df = ds.read_allele_count(regions=["chr1:1-10000"])
    assert df.shape == (7, 7)
    assert sum(df["pos"] == (0, 1, 1, 2, 2, 2, 3)) == 7
    assert sum(df["count"] == (8, 5, 3, 4, 2, 2, 1)) == 7


# Ok to skip is missing bcftools in Windows CI job
@skip_if_no_bcftools
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


def test_create_dataset_extra_attrs(tmp_path):
    """extra_attrs causes those fmt fields to appear in the queryable attribute list."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(extra_attrs=["fmt_GT", "fmt_DP"])
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="r")
    attrs = ds.attributes()
    assert "fmt_GT" in attrs
    assert "fmt_DP" in attrs


def test_create_dataset_extra_attrs_and_vcf_attrs_raises(tmp_path):
    """Providing both extra_attrs and vcf_attrs raises an exception."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    vcf_uri = os.path.join(TESTS_INPUT_DIR, "v2-DjrIAzkP-downsampled.vcf.gz")
    with pytest.raises(Exception, match="Cannot provide both extra_attrs and vcf_attrs"):
        ds.create_dataset(extra_attrs=["fmt_GT"], vcf_attrs=vcf_uri)


def test_create_dataset_invalid_checksum_type_raises(tmp_path):
    """An unrecognised checksum_type raises an exception before touching disk."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    with pytest.raises(Exception, match="Invalid checksum_type"):
        ds.create_dataset(checksum_type="crc32")


def test_create_dataset_checksum_md5(tmp_path):
    """checksum_type='md5' creates a usable dataset."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(checksum_type="md5")
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3


def test_create_dataset_already_exists_raises(tmp_path):
    """Calling create_dataset() a second time on the same URI raises an exception."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()

    ds2 = tiledbvcf.Dataset(uri, mode="w")
    with pytest.raises(Exception):
        ds2.create_dataset()


def test_create_dataset_tile_capacity(tmp_path):
    """A custom tile_capacity creates a usable dataset."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(tile_capacity=100)
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3


def test_create_dataset_anchor_gap(tmp_path):
    """A custom anchor_gap creates a usable dataset."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(anchor_gap=500)
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3


def test_create_dataset_allow_duplicates_false(tmp_path):
    """allow_duplicates=False creates a usable dataset and rejects duplicate ingestion."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(allow_duplicates=False)
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, "small.bcf")])

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3


def test_create_dataset_variant_stats_version2(tmp_path):
    """variant_stats_version=2 (the default) creates a usable dataset."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset(enable_variant_stats=True, variant_stats_version=2)
    ds.ingest_samples(
        [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small3.bcf"]]
    )

    ds = tiledbvcf.Dataset(uri, mode="r")
    df = ds.read_variant_stats(regions=["1:1-200000"])
    assert len(df) > 0


def test_ingest_samples_none_is_noop(tmp_path):
    """Calling ingest_samples() with sample_uris=None returns without error."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples()
    ds.ingest_samples(sample_uris=None)

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 0


def test_ingest_samples_scratch_space_path_only_raises(tmp_path):
    """Providing scratch_space_path without scratch_space_size raises."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    with pytest.raises(Exception, match="Must set both scratch_space_path and scratch_space_size"):
        ds.ingest_samples(
            [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
            scratch_space_path=str(tmp_path),
        )


def test_ingest_samples_scratch_space_size_only_raises(tmp_path):
    """Providing scratch_space_size without scratch_space_path raises."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    with pytest.raises(Exception, match="Must set both scratch_space_path and scratch_space_size"):
        ds.ingest_samples(
            [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
            scratch_space_size=1024,
        )


def test_ingest_samples_invalid_contig_mode_raises(tmp_path):
    """An unrecognised contig_mode raises before ingestion starts."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    with pytest.raises(Exception, match="contig_mode must be"):
        ds.ingest_samples(
            [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
            contig_mode="invalid",
        )


def test_ingest_samples_contigs_to_keep_separate_not_list_raises(tmp_path):
    """Passing a non-list for contigs_to_keep_separate raises."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    with pytest.raises(Exception, match="contigs_to_keep_separate must be a list"):
        ds.ingest_samples(
            [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
            contigs_to_keep_separate="1",
        )


def test_ingest_samples_contigs_to_allow_merging_not_list_raises(tmp_path):
    """Passing a non-list for contigs_to_allow_merging raises."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    with pytest.raises(Exception, match="contigs_to_allow_merging must be a list"):
        ds.ingest_samples(
            [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
            contigs_to_allow_merging="1",
        )


def test_ingest_samples_resume(tmp_path):
    """resume=True is accepted and produces the same result as a normal ingest."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples(
        [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
        resume=True,
    )

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3


def test_ingest_samples_sample_batch_size(tmp_path):
    """sample_batch_size=1 with 2 samples produces 2 fragments (one per batch)."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in ["small.bcf", "small2.bcf"]]
    ds.create_dataset()
    ds.ingest_samples(samples, sample_batch_size=1)

    data_uri = tiledb.Group(uri)["data"].uri
    assert len(tiledb.array_fragments(data_uri)) == 2


def test_ingest_samples_memory_and_thread_params(tmp_path):
    """Memory and thread tuning parameters are accepted and produce correct results."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples(
        [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
        threads=2,
        total_memory_budget_mb=512,
        ratio_tiledb_memory=0.5,
        max_tiledb_memory_mb=256,
        input_record_buffer_mb=2,
        avg_vcf_record_size=512,
        ratio_task_size=0.5,
        ratio_output_flush=0.5,
    )

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3


def test_ingest_samples_total_memory_percentage(tmp_path):
    """total_memory_percentage= is accepted and produces correct results."""
    uri = os.path.join(tmp_path, "dataset")
    ds = tiledbvcf.Dataset(uri, mode="w")
    ds.create_dataset()
    ds.ingest_samples(
        [os.path.join(TESTS_INPUT_DIR, "small.bcf")],
        total_memory_percentage=0.5,
    )

    ds = tiledbvcf.Dataset(uri, mode="r")
    assert ds.count() == 3
