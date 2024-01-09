import pandas as pd
import logging
from typing import List

def split_to_chunks_compreh(df: pd.DataFrame, chunk_size: int, column_name:str='dt', buffer:list=[]) -> List[pd.DataFrame]:
    try:
        chunk_size = int(float(chunk_size))
    except ValueError as e:
        logging.error('Chunk size is not a number')
        logging.error(e)
        return []

    if not isinstance(df, pd.DataFrame):
        logging.error('Input df is not a pandas DataFrame')
        return []

    if len(df) == 0 or chunk_size <= 0 or chunk_size >= len(df):
        return [df]
    
    try:
        counts = df.groupby(column_name).size().to_dict()
    except KeyError as e:
        logging.error(f'Column {e} for chunking is not found')
    
    if chunk_size == 1:
        return [[key] * value for key, value in counts.items()]
    
    chunks = [[]]
    temp = [chunks[-1].extend([k] * v) if len(chunks[-1]) < chunk_size else chunks.append([k] * v) for k, v in counts.items()]
    return chunks