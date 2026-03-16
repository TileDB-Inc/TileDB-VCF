# Copyright (c) TileDB, Inc.
# Licensed under the MIT License.
"""
VCF sample ingestion with Carrara URIs.
"""

from __future__ import annotations

import pytest
import tiledbvcf


@pytest.mark.carrara
def test_ingest_samples(
    carrara_login: None, carrara_group_path: str, vcf_sample_uris: list[str]
) -> None:
    """Ingest two BCF samples into a remote Carrara dataset and verify total count."""
    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()
        ds.ingest_samples(vcf_sample_uris)

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.count() == 14


@pytest.mark.carrara
def test_ingest_and_region_query(
    carrara_login: None, carrara_group_path: str, vcf_sample_uris: list[str]
) -> None:
    """Ingest samples and verify a region-filtered count."""
    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()
        ds.ingest_samples(vcf_sample_uris)

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.count(regions=["1:12700-13400"]) == 6


@pytest.mark.carrara
def test_ingest_and_sample_query(
    carrara_login: None, carrara_group_path: str, vcf_sample_uris: list[str]
) -> None:
    """Ingest samples and verify a sample- and region-filtered count."""
    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()
        ds.ingest_samples(vcf_sample_uris)

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.count(samples=["HG00280"], regions=["1:12700-13400"]) == 4


@pytest.mark.carrara
def test_ingest_incremental(
    carrara_login: None, carrara_group_path: str, vcf_sample_uris: list[str]
) -> None:
    """Ingest one sample, verify count, then add a second sample and re-verify."""
    first, second = vcf_sample_uris[0], vcf_sample_uris[1]

    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()
        ds.ingest_samples([first])

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        count_after_first = ds.count()
        assert count_after_first > 0

    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.ingest_samples([second])

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.count() == 14
        assert ds.count() > count_after_first
