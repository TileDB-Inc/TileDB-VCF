# Copyright (c) TileDB, Inc.
# Licensed under the MIT License.

from __future__ import annotations

import os
from collections.abc import Generator
from uuid import uuid4

import tiledbvcf
import tiledb
import pytest


# Base CARRARA URI used for all tests.
# - The teamspace must be owned by the test user.
# - The workspace must match the workspace defined in the profile.
PROFILE_NAME = os.getenv("CARRARA_TEST_PROFILE") or "qa"
WORKSPACE_NAME = os.getenv("CARRARA_TEST_WORKSPACE") or "TileDB-Inc."
TEAMSPACE_NAME = os.getenv("CARRARA_TEST_TEAMSPACE") or "uat-tests"
TEST_FOLDER = os.getenv("CARRARA_TEST_FOLDER") or "remote_test"
BASE_URI = f"tiledb://{WORKSPACE_NAME}/{TEAMSPACE_NAME}/{TEST_FOLDER}"


@pytest.fixture(scope="session")
def carrara_login() -> None:
    import tiledb.client

    tiledb.client.login(profile_name=PROFILE_NAME)
    assert tiledb.client.workspaces.get_workspace(tiledb.client.client.get_workspace_id()).name == WORKSPACE_NAME


@pytest.fixture
def carrara_array_path() -> Generator[str, None, None]:
    """Fixture returns an Array path that will be recursively deleted after test finishes."""
    import tiledb.client

    path = f"{BASE_URI}/{uuid4()}"
    yield path

    tiledb.client.assets.delete_asset(path, delete_storage=True)


@pytest.fixture
def carrara_group_path() -> Generator[str, None, None]:
    """Fixture returns a Group path that will be recursively deleted after test finishes."""
    path = f"{BASE_URI}/{uuid4()}"
    yield path

    try:
        with tiledb.Group(path, mode="m") as G:
            G.delete(recursive=True)
    except tiledb.TileDBError:
        pass


