import os

import pytest
import tiledb
import tiledbvcf

from .conftest import skip_if_incompatible, TESTS_INPUT_DIR

@pytest.mark.parametrize("compress", [True, False])
def test_sample_compression(tmp_path, compress):
    # Create the dataset
    dataset_uri = os.path.join(tmp_path, "sample_compression")
    array_uri = os.path.join(dataset_uri, "data")
    ds = tiledbvcf.Dataset(dataset_uri, mode="w")
    ds.create_dataset(compress_sample_dim=compress)

    skip_if_incompatible(array_uri)

    # Check for the presence of the Zstd filter
    found_zstd = False
    with tiledb.open(array_uri) as A:
        for filter in A.domain.dim("sample").filters:
            found_zstd = found_zstd or "Zstd" in str(filter)

    assert found_zstd == compress


@pytest.mark.parametrize("level", [1, 4, 16, 22])
def test_compression_level(tmp_path, level):
    # Create the dataset
    dataset_uri = os.path.join(tmp_path, "compression_level")
    array_uri = os.path.join(dataset_uri, "data")
    ds = tiledbvcf.Dataset(dataset_uri, mode="w")
    ds.create_dataset(compression_level=level)

    skip_if_incompatible(array_uri)

    # Check for the expected compression level
    with tiledb.open(array_uri) as A:
        for i in range(A.schema.nattr):
            attr = A.schema.attr(i)
            for filter in attr.filters:
                if "Zstd" in str(filter):
                    assert filter.level == level


