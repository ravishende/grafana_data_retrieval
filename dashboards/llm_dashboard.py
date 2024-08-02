# autopep8: off
import pandas as pd
import shutil
import time
import sys
import os
sys.path.append("../grafana_data_retrieval")  # Adjust the path to go up one level
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from graphs import Graphs
from graph_visualization import display_graphs
from helpers.printing import print_dataframe_dict
from helpers.time_functions import datetime_ify
from helpers.querying import query_data
# autopep8: on

'''
==========================================================================================
TODO:
Fan Speed doesn't seem to be tracked by the pod on our ndp namespace. All of the other 
queries work, but when you specify the same pod prefix for the fan speed, there are no
results. However, if you specify any pod, there are many many results.
To verify if this is true, first find the Node that corresponds to this pod
    pod name: "fc-worker-1-64dbc56b5c-qnqpk"
    node name = ?
Then use that as the Host in the following Grafana dashbaord
    Grafana dashboard: https://grafana.nrp-nautilus.io/d/Tf9PkuSik/k8s-nvidia-gpu-node?orgId=1&refresh=15m&from=now-3h&to=now&var-interval=1m&var-host=fc-worker-1-64dbc56b5c-qnqpk
==========================================================================================
'''

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

# query settings
filter_graphs_for_pod = False

# optional filtering for pods with a given prefix
filter_pod_str = ""
if filter_graphs_for_pod:
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

'''
================================================
                Querying over time
================================================
'''
READ_FILE = "csvs/queried2_df.csv"
SAVE_FILE = "csvs/queried3_df.csv"


# get the queried df from the save file, or an empty df otherwise
def _get_queried_df():
    if os.path.exists(SAVE_FILE):
        return pd.read_csv(SAVE_FILE)
    return pd.DataFrame()


# query a row in a dataframe
def query_row(row):
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
        print(f"No value for", title)
    return row


# query a chunk of a dataframe (n rows)
def query_chunk(df_chunk, track_time=True):
    # query df chunk
    start = time.time()
    df_chunk = df_chunk.apply(
        lambda row: query_row(row), axis=1)
    end = time.time()
    # print elapsed time info
    if track_time:
        elapsed_time = end - start
        print("\n\nelapsed time for batch:",
              round(elapsed_time, 2), "seconds\n\n")
    return df_chunk


# given a df and optional batch size,
# return a fully queried df, saving progress every batch
def query_df(df, batch_size=100, track_batch_times=True):
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
        queried_df.to_csv(SAVE_FILE)

    return queried_df


df = pd.read_csv(READ_FILE, index_col=0)


# get times back to datetimes from strings
df['start'] = df['start'].apply(datetime_ify)
df['end'] = df['end'].apply(datetime_ify)

queried_df = query_df(df)
print(queried_df)
