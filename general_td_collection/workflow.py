"""This file collects performance data of runs in a dataframe

Given a dataframe with 'start' and 'end' columns (and potentially others too),
Query it over those times with selected queries and metrics from grafana dashboards
You can specify a filter for the queries as well as which dashboards you would life for querying.
The filter can be a node, pod, namespace, or regex for any of those.
Additionally, for graph queries, you can specify which metrics you would like saved 
(e.g. max, increase, etc.).

Usage:
1. Specify the filter when instantiating the QueryHandler class
    - query_handler = QueryHandler(pod_regex="...", namespace="wifire-quicfire")
    - If you later need to redefine filters in the same program, use the update_filter_str() method
2. Call the query_df method, specifying which query dashboards to include
    - queried_df = query_handler.query_df(df, rgw_queries=True,gpu_queries=True)
3. In the Finalizer's sum_df method, specify the graph_metrics to collect for graph queries
    - finalizer.sum_df(queried_df, graph_metrics=['max', 'mean', 'increase'])
4. Save the final dataframe of queried information
    - df.to_csv()
"""
# autopep8: off
import shutil
import pandas as pd
from setup import prompt_new_run
from querying import QueryHandler
from finalizing import Finalizer
# autopep8: on

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# SETTINGS
READ_FILE = "csvs/read.csv"
WRITE_FILE = "csvs/write.csv"
# Set to False if continuing to query, otherwise, set to True
NEW_RUN = True

# ==========================
#       MAIN PROGRAM
# ==========================

# get the df & make sure there's no unnamed column from if csv was saved with/without an index col
df = pd.read_csv(READ_FILE)
unnamed_cols = df.columns.str.match('Unnamed')
df = df.loc[:, ~unnamed_cols]

prompt_new_run(NEW_RUN)

# way to filter - can be pod, node, namespace, or regex of any of the three
NODE_NAME = "node-1-1.sdsc.optiputer.net"
POD_PREFIX = 'fc-worker-1-'
pod_regex_str = f'^{POD_PREFIX}.*'

# initialize classes with desired filter and data settings
query_handler = QueryHandler(node=NODE_NAME)
# query_handler = QueryHandler(pod_regex=pod_regex_str)
# query_handler = QueryHandler(namespace="rook")  # has data for rgw queries
finalizer = Finalizer()

df = df.iloc[len(df)-7:]
print("\n\n\nStarting df:\n", df, "\n\n\n\n")

# Main workflow
df = query_handler.query_df(df,
                            rgw_queries=False,
                            gpu_queries=True,
                            gpu_compute_resource_queries=True,
                            cpu_compute_resource_queries=True)
df = finalizer.sum_df(
    df, graph_metrics=['min', 'max', 'mean', 'median', 'increase'])


df.to_csv(WRITE_FILE)
print(f"Finalized dataframe:\n{df}")
