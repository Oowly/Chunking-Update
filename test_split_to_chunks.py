import pandas as pd
from split_to_chunks_group_by import split_to_chunks_group_by
import pytest

@pytest.fixture
def example_dataframe1():
    dfs = pd.date_range(
    "2023-01-01 00:00:00", "2023-04-01 00:01:00", freq="s"
    )
    return pd.DataFrame({"dt": dfs.repeat(10)})

@pytest.fixture
def example_dataframe2():
    dfs = pd.date_range(
    "2023-01-01 00:00:00", "2023-12-01 00:01:00", freq="s"
    )
    return pd.DataFrame({"dt": dfs.repeat(15)})

def test_split_to_chunks_group_by_invalid_chunk_size(example_dataframe1, example_dataframe2):
    chunk_size = 'invalid'
    result1 = split_to_chunks_group_by(example_dataframe1, chunk_size)
    result2 = split_to_chunks_group_by(example_dataframe2, chunk_size)
    assert result1 == [] and result2 == []

def test_split_to_chunks_group_by_invalid_dataframe_type():
    chunk_size = 2
    result = split_to_chunks_group_by("not_dataframe", chunk_size)
    assert result == []

def test_split_to_chunks_group_by_empty_dataframe():
    df = pd.DataFrame()
    chunk_size = 2
    result = split_to_chunks_group_by(df, chunk_size)
    assert result == [df]

def test_split_to_chunks_group_by_single_chunk(example_dataframe1, example_dataframe2):
    chunk_size = len(example_dataframe2)
    result1 = split_to_chunks_group_by(example_dataframe1, chunk_size)
    result2 = split_to_chunks_group_by(example_dataframe2, chunk_size)
    assert len(result1) == 1 and result1[0].equals(example_dataframe1)
    assert len(result2) == 1 and result2[0].equals(example_dataframe2)

def test_split_to_chunks_group_by_empty_chunk(example_dataframe1, example_dataframe2):
    chunk_size = 0
    result1 = split_to_chunks_group_by(example_dataframe1, chunk_size)
    result2 = split_to_chunks_group_by(example_dataframe2, chunk_size)
    assert result1 == [example_dataframe1] and result2 == [example_dataframe2]