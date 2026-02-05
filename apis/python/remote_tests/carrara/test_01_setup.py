# Copyright (c) TileDB, Inc.
# Licensed under the MIT License.
"""
Basic object creation with Carrara URIs.
"""


from __future__ import annotations

import tiledbvcf

def test_dataset_create(carrara_login: None, carrara_group_path: str):
    with tiledbvcf.Dataset(carrara_group_path, mode="w") as ds:
        ds.create_dataset()

    with tiledbvcf.Dataset(carrara_group_path, mode="r") as ds:
        assert ds.count() == 0
