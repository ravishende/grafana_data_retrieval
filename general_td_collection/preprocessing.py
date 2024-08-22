# autopep8: off
import pandas as pd
import sys
import os
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.time_functions import datetime_ify
# autopep8: on


def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    if not 'start' in df.columns or not 'end' in df.columns:
        raise ValueError(
            "dataframe must have a 'start' column and an 'end' columnn")

    # deal with times and create runtime column
    df['start'] = df['start'].apply(datetime_ify)
    df['end'] = df['end'].apply(datetime_ify)
    df['runtime'] = (df['end'] - df['start']).dt.total_seconds()
    df['runtime'] = df['runtime'].round()

    return df
