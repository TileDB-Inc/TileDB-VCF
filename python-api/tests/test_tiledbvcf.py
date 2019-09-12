import numpy as np
import os
import pandas as pd
import pytest
import tiledbvcf

# Directory containing this file
CONTAINING_DIR = os.path.abspath(os.path.dirname(__file__))

# Test inputs directory
TESTS_INPUT_DIR = os.path.abspath(
    os.path.join(CONTAINING_DIR, '../../test/inputs'))


def _check_dfs(expected, actual):
    for k in expected:
        assert expected[k].equals(actual[k])
    for k in actual:
        assert actual[k].equals(expected[k])


@pytest.fixture
def test_ds():
    return tiledbvcf.TileDBVCFDataset(
        os.path.join(TESTS_INPUT_DIR, 'arrays/ingested_2samples'))


def test_basic_count(test_ds):
    assert test_ds.count() == 14


def test_read_must_specify_attrs(test_ds):
    with pytest.raises(Exception):
        df = test_ds.read()


def test_basic_reads(test_ds):
    expected_df = pd.DataFrame(
        {'sample_name': pd.Series(
            ['HG00280', 'HG01762', 'HG00280', 'HG01762', 'HG00280',
             'HG01762', 'HG00280', 'HG00280', 'HG00280', 'HG00280',
             'HG00280', 'HG00280', 'HG00280', 'HG00280']),
            'pos_start': pd.Series(
                [12141, 12141, 12546, 12546, 13354, 13354, 13375, 13396,
                 13414, 13452, 13520, 13545, 17319, 17480], dtype=np.int32),
            'pos_end': pd.Series(
                [12277, 12277, 12771, 12771, 13374, 13389, 13395, 13413,
                 13451, 13519, 13544, 13689, 17479, 17486], dtype=np.int32)})
    df = test_ds.read(attrs=['sample_name', 'pos_start', 'pos_end'])
    _check_dfs(expected_df, df)

    # Region intersection
    df = test_ds.read(attrs=['sample_name', 'pos_start', 'pos_end'],
                      regions=['1:12700-13400'])
    expected_df = pd.DataFrame(
        {'sample_name': pd.Series(
            ['HG00280', 'HG01762', 'HG00280', 'HG01762', 'HG00280', 'HG00280']),
            'pos_start': pd.Series([12546, 12546, 13354, 13354, 13375, 13396],
                                   dtype=np.int32),
            'pos_end': pd.Series([12771, 12771, 13374, 13389, 13395, 13413],
                                 dtype=np.int32)})
    _check_dfs(expected_df, df)

    # Region and sample intersection
    df = test_ds.read(attrs=['sample_name', 'pos_start', 'pos_end'],
                      regions=['1:12700-13400'], samples=['HG01762'])
    expected_df = pd.DataFrame(
        {'sample_name': pd.Series(['HG01762', 'HG01762']),
         'pos_start': pd.Series([12546, 13354], dtype=np.int32),
         'pos_end': pd.Series([12771, 13389], dtype=np.int32)})
    _check_dfs(expected_df, df)

    # Sample only
    df = test_ds.read(attrs=['sample_name', 'pos_start', 'pos_end'],
                      samples=['HG01762'])
    expected_df = pd.DataFrame(
        {'sample_name': pd.Series(['HG01762', 'HG01762', 'HG01762']),
         'pos_start': pd.Series([12141, 12546, 13354], dtype=np.int32),
         'pos_end': pd.Series([12277, 12771, 13389], dtype=np.int32)})
    _check_dfs(expected_df, df)


def test_multiple_counts(test_ds):
    assert test_ds.count() == 14
    assert test_ds.count() == 14
    assert test_ds.count(regions=['1:12700-13400']) == 6
    assert test_ds.count(samples=['HG00280'], regions=['1:12700-13400']) == 4
    assert test_ds.count() == 14
    assert test_ds.count(samples=['HG01762']) == 3
    assert test_ds.count(samples=['HG00280']) == 11


def test_empty_region(test_ds):
    assert test_ds.count(regions=['12:1-1000000']) == 0


def test_missing_sample_raises_exception(test_ds):
    with pytest.raises(RuntimeError):
        test_ds.count(samples=['abcde'])


def test_bad_contig_name_raises_exception(test_ds):
    with pytest.raises(RuntimeError):
        test_ds.count(regions=['chr1:1-1000000'])


def test_incomplete_reads(test_ds):
    test_ds.reader.set_buffer_alloc_size(10)
    df = test_ds.read(attrs=['pos_end'], regions=['1:12700-13400'])
    assert not test_ds.read_completed()
    assert len(df) == 2
    _check_dfs(pd.DataFrame.from_dict(
        {'pos_end': np.array([12771, 12771], dtype=np.int32)}), df)

    df = test_ds.continue_read()
    assert not test_ds.read_completed()
    assert len(df) == 2
    _check_dfs(pd.DataFrame.from_dict(
        {'pos_end': np.array([13374, 13389], dtype=np.int32)}), df)

    df = test_ds.continue_read()
    assert test_ds.read_completed()
    assert len(df) == 2
    _check_dfs(pd.DataFrame.from_dict(
        {'pos_end': np.array([13395, 13413], dtype=np.int32)}), df)


def test_incomplete_read_generator(test_ds):
    test_ds.reader.set_buffer_alloc_size(10)

    overall_df = None
    for df in test_ds.read_iter(attrs=['pos_end'], regions=['1:12700-13400']):
        if overall_df is None:
            overall_df = df
        else:
            overall_df = overall_df.append(df, ignore_index=True)

    assert len(overall_df) == 6
    _check_dfs(pd.DataFrame.from_dict(
        {'pos_end': np.array([12771, 12771, 13374, 13389, 13395, 13413],
                             dtype=np.int32)}), overall_df)


def test_read_filters(test_ds):
    df = test_ds.read(attrs=['sample_name', 'pos_start', 'pos_end', 'filters'],
                      regions=['1:12700-13400'])
    expected_df = pd.DataFrame(
        {'sample_name': pd.Series(
            ['HG00280', 'HG01762', 'HG00280', 'HG01762', 'HG00280', 'HG00280']),
            'pos_start': pd.Series([12546, 12546, 13354, 13354, 13375, 13396],
                                   dtype=np.int32),
            'pos_end': pd.Series([12771, 12771, 13374, 13389, 13395, 13413],
                                 dtype=np.int32),
            'filters': pd.Series([None, None, 'LowQual', None, None, None])})
    _check_dfs(expected_df, df)


def test_basic_ingest(tmp_path):
    # Create the dataset
    uri = os.path.join(tmp_path, 'dataset')
    ds = tiledbvcf.TileDBVCFDataset(uri, mode='w')
    samples = [os.path.join(TESTS_INPUT_DIR, s) for s in
               ['small.bcf', 'small2.bcf']]
    ds.ingest_samples(samples)

    # Open it back in read mode and check some queries
    ds = tiledbvcf.TileDBVCFDataset(uri, mode='r')
    assert ds.count() == 14
    assert ds.count(regions=['1:12700-13400']) == 6
    assert ds.count(samples=['HG00280'], regions=['1:12700-13400']) == 4


def test_incremental_ingest(tmp_path):
    uri = os.path.join(tmp_path, 'dataset')
    ds = tiledbvcf.TileDBVCFDataset(uri, mode='w')
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, 'small.bcf')])
    ds.ingest_samples([os.path.join(TESTS_INPUT_DIR, 'small2.bcf')])

    # Open it back in read mode and check some queries
    ds = tiledbvcf.TileDBVCFDataset(uri, mode='r')
    assert ds.count() == 14
    assert ds.count(regions=['1:12700-13400']) == 6
    assert ds.count(samples=['HG00280'], regions=['1:12700-13400']) == 4
