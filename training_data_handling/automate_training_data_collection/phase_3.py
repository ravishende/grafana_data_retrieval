import pandas as pd
import shutil
from workflow_files import phase3_files
from itertools import chain
import sys
import os
sys.path.append("../../grafana_data_retrieval")  # Adjust the path to go up two levels
parent = os.path.dirname(os.path.realpath(__file__))
grandparent = os.path.dirname(parent)
sys.path.append(grandparent)
from query_resources import query_and_insert_columns

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


'''
==========================
    Helper functions
==========================
'''

NUM_DURATION_COLS = None

def _get_num_duration_cols():
    global NUM_DURATION_COLS
    if NUM_DURATION_COLS is not None:
        return NUM_DURATION_COLS

    with open("num_duration_cols.txt","r") as f:
        num_duration_cols = int(f.read())
    
    NUM_DURATION_COLS = num_duration_cols
    return num_duration_cols



# get the static and non_static metrics lists
def _get_metrics():
    # define the metrics to be queried
    # list of all metrics you can query (with query_and_insert_columns())
    all_metrics = [
        "cpu_usage",
        "mem_usage",
        "cpu_request",
        "mem_request",
        "transmitted_packets",
        "received_packets",
        "transmitted_bandwidth",
        "received_bandwidth"
        ]
    # metrics that don't change over a run
    static_metrics = ["cpu_request", "mem_request"]
    # metrics that do change over a run
    non_static_metrics = [metric for metric in all_metrics if metric not in static_metrics]

    return static_metrics, non_static_metrics


def _get_metric_column_names():
    static_metrics, non_static_metrics = _get_metrics()
    # get column names
    col_names_static = static_metrics
    col_names_total = [name + "_total" for name in non_static_metrics]
    num_duration_cols = _get_num_duration_cols()

    # get col_names_t1, col_names_t2, etc. in a list called col_names_by_time
    col_names_by_time = []
    for i in range(1, num_duration_cols):
        col_names_t_i = [name + "_t" + str(i) for name in non_static_metrics]
        col_names_by_time.append(col_names_t_i)
    return col_names_static, col_names_total, col_names_by_time

# given: 
    # df - a dataframe 
    # batch_size - number of rows to query at a time until the df is filled out
    # temporary_save_file - name of a csv file to save df to after each big insert in case the program is stopped
# query all important metrics, saving to the temporary_save_file after inserting columns of the same duration column.
    # Note: this function assumes the total duration column is "runtime" and duration columns 
    # are in the form "duration_t{N}" where {N} is an int from 1 to num_duration_cols inclusive
# return the updated dataframe with all columns queried
def query_metrics(df, batch_size, temporary_save_file):
    # get metrics lists and number of duration columns
    static_metrics, non_static_metrics = _get_metrics()
    num_duration_cols = _get_num_duration_cols()

    # get duration column names
    duration_col_names = ["duration_t" + str(num) for num in range(1,num_duration_cols+1)]
    duration_col_total = "runtime"

    # get metric column names
    col_names_static, col_names_total, col_names_by_time = _get_metric_column_names()

    # collect all metric column names and initialize them in the dataframe if they aren't already
    all_col_names = col_names_static + col_names_total + list(chain.from_iterable(col_names_by_time))

    for col_name in all_col_names:
        if col_name not in df.columns:
            df[col_name] = None

    # while there are still unqueried rows, keep querying batch_size rows at a time
    while df[col_names_total[0]].iloc[len(df)-1] is None:
        # query and insert static and total columns
        df = query_and_insert_columns(df, static_metrics, col_names_static, duration_col_total, batch_size)
        df.to_csv(temporary_save_file)
        df = query_and_insert_columns(df, non_static_metrics, col_names_total, duration_col_total, batch_size)
        df.to_csv(temporary_save_file)
        # query and insert duration_t_i columns
        for i, col_names_t_i in enumerate(col_names_by_time):
            df = query_and_insert_columns(df, non_static_metrics, col_names_t_i, duration_col_names[i], batch_size)
            df.to_csv(temporary_save_file)

    return df



'''
======================
    Main Program
======================
'''

# get preprocessed_df
preprocessed_df = pd.read_csv(phase3_files['read'], nrows=3)

# 7. query resource metrics (metrics total, t1, t2)
temporary_save_file = "csvs/query_progress.csv"
rows_batch_size = 20
queried_df = query_metrics(preprocessed_df, rows_batch_size, temporary_save_file)

# save df to a csv file
queried_df.to_csv(phase3_files['write'])
print(queried_df)
