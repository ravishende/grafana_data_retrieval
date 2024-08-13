# autopep8: off
import pandas as pd
import random
import sys
import os
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.time_functions import datetime_ify
# autopep8: on


# generate random values between run_start and some end time, put into duration1
def insert_rand_refresh_col(df: pd.DataFrame, refresh_title: str, method: int = 0) -> pd.DataFrame:
    duration_seconds = df['runtime']

    if method == 0:
        # generate random values between 45sec and 5min
        df[refresh_title] = duration_seconds.apply(
            lambda time: random.randint(45, 300) if time >= 300 else time)
    elif method == 1:
        # generate random values between 45sec and half of the duration
        df[refresh_title] = duration_seconds.apply(
            lambda time: random.randint(45, time // 2) if time // 2 >= 45 else time)
    elif method == 2:
        # generate random values between 45sec and the full duration
        df[refresh_title] = duration_seconds.apply(
            lambda time: random.randint(45, time) if time >= 45 else time)
    else:
        raise ValueError("method must be: 0, 1, or 2")
    return df


def preprocess_df(df: pd.DataFrame, num_refresh_cols: int = 0) -> pd.DataFrame:
    # deal with times and create runtime column
    df['start'] = df['start'].apply(datetime_ify)
    df['end'] = df['end'].apply(datetime_ify)
    df['runtime'] = (df['end'] - df['start']).dt.total_seconds()
    df['runtime'] = df['runtime'].round()

    for i in range(num_refresh_cols):
        method = i
        if method > 2:
            method = 2
        insert_rand_refresh_col(df, f"duration_t{i+1}", method=method)

    return df
