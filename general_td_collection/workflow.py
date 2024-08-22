# autopep8: off
import shutil
import pandas as pd
from setup import prompt_new_run
from preprocessing import preprocess_df
from querying import QueryHandler
from finalizing import Finalizer
# autopep8: on

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# File purpose:
# given a dataframe with 'start' and 'end' columns (and potentially others too),
# query it over those times with selected queries and metrics


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
POD_PREFIX = 'fc-worker-1-'
pod_regex_str = f'^{POD_PREFIX}.*'

# initialize classes with desired filter and data settings
query_handler = QueryHandler(node="node-1-1.sdsc.optiputer.net")
# query_handler = Query_handler(pod_regex=pod_regex_str)
finalizer = Finalizer()

df = df.iloc[len(df)-14:]
print("\n\n\nStarting df:\n", df, "\n\n\n\n")

# Main workflow
df = preprocess_df(df)
df = query_handler.query_df(df,
                            rgw_queries=False,
                            gpu_queries=True,
                            gpu_compute_resource_queries=True,
                            cpu_compute_resource_queries=True)
df = finalizer.sum_df(
    df, graph_metrics=['min', 'max', 'mean', 'median', 'increase'])


df.to_csv(WRITE_FILE)
print(f"Finalized dataframe:\n{df}")
