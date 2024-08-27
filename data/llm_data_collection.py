# autopep8: off
import shutil
import time
import sys
import os
import pandas as pd
# pylint: disable=wrong-import-position
sys.path.append("../grafana_data_retrieval")  # Adjust the path to go up one level
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from graphs import Graphs
from helpers.time_functions import datetime_ify
# autopep8: on


# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

# query settings
FILTER_GRAPHS_FOR_POD = False

# optional filtering for pods with a given prefix
# pylint: disable='invalid-name'
filter_pod_str = ""
if FILTER_GRAPHS_FOR_POD:
    pod_prefix = 'fc-worker-1-'
    filter_pod_str = f', pod=~"^{pod_prefix}.*"'

queries = {
    'GPU Utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node!=""' + filter_pod_str + '}',
    'Memory Copy Utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node!=""' + filter_pod_str + '}',
    'Power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node!=""' + filter_pod_str + '}',
    'Temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node!=""' + filter_pod_str + '}',
    'Fan Speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node!=""' + filter_pod_str + '}'
}

# get graphs class
graphs_class = Graphs()

# constants
READ_FILE = "llm/stress_tests.csv"
QUERIED_SAVE_FILE = "llm/queried_df.csv"
FINAL_SAVE_FILE = "llm/llm_data.csv"


# get the queried df from the save file, or an empty df otherwise
def _get_queried_df():
    if os.path.exists(QUERIED_SAVE_FILE):
        return pd.read_csv(QUERIED_SAVE_FILE)
    return pd.DataFrame()


# query a row in a dataframe
def query_row(row: pd.Series) -> pd.Series:
    # get dict containing all queried graphs
    graphs_dict = graphs_class.get_graphs_from_queries(
        queries, start=row['start'], end=row['end'], sum_by=["namespace", "pod"])
    # get dict of single average for all graphs
    for title, df in graphs_dict.items():
        if df is not None:
            row[f'Average {title}'] = df[title].mean()
            row[f'Max {title}'] = df[title].max()
            row[f'Min {title}'] = df[title].min()
            row[f'Standard Deviation {title}'] = df[title].std()
            continue

        row[f'Average {title}'] = None
        row[f'Max {title}'] = None
        row[f'Min {title}'] = None
        row[f'Standard Deviation {title}'] = None
        print("No value for", title)
    return row


# query a chunk of a dataframe (n rows)
def query_chunk(df_chunk: pd.DataFrame, track_time: bool = True) -> pd.DataFrame:
    # query df chunk
    start = time.time()
    df_chunk = df_chunk.apply(query_row, axis=1)
    end = time.time()
    # print elapsed time info
    if track_time:
        elapsed_time = end - start
        print("\n\nelapsed time for batch:",
              round(elapsed_time, 2), "seconds\n\n")
    return df_chunk


# given a df and optional batch size,
# return a fully queried df, saving progress every batch
def query_df(df: pd.DataFrame, batch_size: int = 100, track_batch_times: bool = True) -> pd.DataFrame:
    # get the queried df and remove already queried rows from df_to_query
    queried_df = _get_queried_df()
    queried_rows = len(queried_df)

    # query df in chunks
    while queried_rows < len(df):
        # get batch
        df_to_query = df.iloc[queried_rows:].reset_index(drop=True)
        batch_end = min(batch_size, len(df_to_query))
        df_chunk = df_to_query.iloc[:batch_end]
        print(
            f"Querying rows {queried_rows} to {queried_rows+len(df_chunk)-1}")
        # query batch
        queried_chunk = query_chunk(df_chunk, track_time=track_batch_times)
        queried_df = pd.concat([queried_df, queried_chunk], ignore_index=True)
        queried_rows += len(queried_chunk)
        queried_df.to_csv(QUERIED_SAVE_FILE)

    return queried_df


# given a column name, rename it to remove spaces, make lowercase, and shorten words
def rename_col(column: str) -> str:
    column = column.lower()
    column = column.replace(" ", "_")
    column = column.replace("memory", "mem")
    column = column.replace("standard deviation", "std")
    column = column.replace("average", "avg")
    return column


og_df = pd.read_csv(READ_FILE)
# get times back to datetimes from strings
og_df['start'] = og_df['start'].apply(datetime_ify)
og_df['end'] = og_df['end'].apply(datetime_ify)

# querying - takes a while
fully_queried_df = query_df(og_df)

# take a queried dataframe and get it ready for machine learning
rename_dict = {col: rename_col(col) for col in fully_queried_df.columns}
final_df = fully_queried_df.rename(columns=rename_dict)
final_df = final_df.drop(columns=['start', 'end', 'question_method',
                                  'pdf_pages', 'pdf_load_time'])

final_df.to_csv(FINAL_SAVE_FILE)
print(final_df)
