# Copyright (c) TileDB, Inc.
# Licensed under the MIT License.
"""
Dataset and sample deletion with Carrara URIs.
"""

from __future__ import annotations

import pytest
import tiledb
import tiledbvcf


@pytest.mark.carrara
def test_delete_dataset(
    carrara_login: None, carrara_group_path: str, vcf_sample_uris: list[str]
) -> None:
    """Create and ingest a dataset, then delete it and verify it no longer exists."""
    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()
        ds.ingest_samples(vcf_sample_uris)

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.count() == 14

    with tiledb.Group(carrara_group_path, mode="m") as G:
        G.delete(recursive=True)

    assert tiledb.object_type(carrara_group_path) is None


@pytest.mark.carrara
def test_delete_samples(
    carrara_login: None, carrara_group_path: str, vcf_sample_uris: list[str]
) -> None:
    """Ingest two samples, delete one, and verify only the other remains."""
    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()
        ds.ingest_samples(vcf_sample_uris)

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert sorted(ds.samples()) == ["HG00280", "HG01762"]

    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.delete_samples(["HG00280"])

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.samples() == ["HG01762"]
