import pandas as pd 
import json
from ast import literal_eval
from termcolor import colored
from uuid import UUID
import shutil
import sys
import os
# get set up to be able to import helper files from parent directory (grafana_data_retrieval)
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("resource_json_summation.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.filtering import get_worker_id
from helpers.printing import print_heading

'''
NOTE:
This file should be run after find_ensembles.py creates a new csv 
of the training data with an included ensemble_id column.
This file assumes there is already a column called 'ensemble'
that contains the ensemble that each run is a part of.
'''

# settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)
# pd.set_option("display.max_rows", None)

read_file = "csv_files/queried.csv" 
# read_file = "csv_files/updated_requests.csv" 
write_file = "csv_files/summed.csv"
# write_file = "csv_files/updated_requests.csv"
# success_write_file = "csv_files/summed_success.csv"
# na_write_file = "csv_files/summed_na.csv"


def get_columns_from_metrics(metric_list):
    summary_columns = []
    for name in metric_list:
        summary_columns.append(name + "_total")
        summary_columns.append(name + "_t1")
        summary_columns.append(name + "_t2")
    return summary_columns


# Given a json result_list (json-like data) of a queried metric (cpu or mem usage),
# Return:
    # a float summation over all pods' values in that ensemble (if all conditions are met)
        # or -1 if no pods match ensemble_id
    # result_list (if there is no ensemble id provided)
    # None (if there are no bp3d-workers to sum over in result_list)
def sum_pods_for_ensemble(result_list, ensemble, static=False):
    # handle if there is no ensemble id
    if not ensemble:
        return result_list

    total = 0
    worker_found = False
    # loop over pods in ensemble, adding values to sum
    for item in result_list:
        # get worker id of each pod
        pod = item["metric"]["pod"]
        worker_id = get_worker_id(pod)

        # skip pod if it's not a bp3d-worker-pod
        if worker_id is None:
            continue

        # if ensemble id matches run's ensemble, add it to total
        if str(UUID(worker_id)) == ensemble:
            value = item["value"][1]
            total += float(value)
            worker_found = True
            if static:
                return total

        # if there are no worker pods that match the ensemble id, return -1
        if total == 0 and not worker_found:
            total = -1

    return total

# given a dataframe, title of a column to update, and title of the ensemble_id column,
# return a new dataframe with the edited column being float values instead of json data
# in every row where there was an ensemble id in the ensemble column, or None if there are
# no bp3d-worker-pods
def update_col(df, update_col_title, ensemble_col_title, static=False):
    # drop na values of column
    df = df.dropna(subset=update_col_title)

    # get result_list as list (not string)
    df[update_col_title] = df[update_col_title].apply(literal_eval)

    # calculate totals summed over the ensemble for given column
    df[update_col_title] = df.apply(
        lambda row: sum_pods_for_ensemble(row[update_col_title], row[ensemble_col_title], static=False) \
        if row[ensemble_col_title] else row[update_col_title],axis=1)

    return df

# given a dataframe, list of column names to update, and title of the ensemble_id column,
# return a new dataframe with the edited columns being float values of the summed over json data
def update_columns(df, update_col_names, ensemble_col_title, static=False):
    num_update_cols = len(update_col_names)
    for i, col_name in enumerate(update_col_names):
        print(f"\n\nUpdating {col_name}", colored(f"({i+1}/{num_update_cols})", "green"))
        df = update_col(df, col_name, ensemble_col_title, static=static)
    return df



'''
============================================
                Main Program
============================================
'''


# all queried metrics
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

static_metrics = [
    "cpu_request",
    "mem_request",
]

metrics_to_sum = [metric for metric in all_metrics if metric not in static_metrics]
# get all columns to sum
columns_to_sum = get_columns_from_metrics(metrics_to_sum)

# select columns to update (sum over) as well as ensemble ids column
ensemble_col = "ensemble_uuid"

# get the csv file as a pandas dataframe
summed_runs = pd.read_csv(read_file, index_col=0)

# update columns to get float values from json
print_heading("Summing Up Columns")
summed_runs = update_columns(summed_runs, columns_to_sum, ensemble_col, static=False)
print_heading("Getting Floats for Static Columns")
summed_runs = update_columns(summed_runs, static_metrics, ensemble_col, static=True)

# get percentage metrics in a format of [%_col, numerator_col, denominator_col]
percentage_column_formats = [
    ["cpu_request_%_total", "cpu_usage_total", "cpu_request"],
    ["cpu_request_%_t1", "cpu_usage_t1", "cpu_request"],
    ["cpu_request_%_t2", "cpu_usage_t2", "cpu_request"],
    ["mem_request_%_total", "mem_usage_total", "mem_request"],
    ["mem_request_%_t1", "mem_usage_t1", "mem_request"],
    ["mem_request_%_t2", "mem_usage_t2", "mem_request"],
]
# calculate percentage columns
for metric_list in percentage_column_formats:
    summed_runs[metric_list[0]] = 100 * summed_runs[metric_list[1]] / summed_runs[metric_list[2]]

summed_runs.to_csv(write_file)
print(summed_runs)


'''
# split the summed_runs into runs that had data for total resources and for ones that didn't (no bp3d-workers)
# in other words, if cpu_tot or mem_tot are none, add row to na_mask
# na_mask = summed_runs[cpu_tot].isna() | summed_runs[mem_tot].isna()
# na_worker_runs = summed_runs[na_mask]
# valid_worker_runs = summed_runs[~na_mask]

# summed_runs = update_col(summed_runs, "cpu_request_total", ensemble_col, static=True)
# summed_runs = update_col(summed_runs, "mem_request_total", ensemble_col, static=True)
# save dataframes to new files and print summed_runs

# valid_worker_runs.to_csv(success_write_file)
# na_worker_runs.to_csv(na_write_file)

# can be used to find the worker ids of each run for analysis/debugging purposes
# def get_ids(res_list):
#     ids = []
#     for item in res_list:
#         ids.append(get_worker_id(item['metric']['pod']))
#     return ids


# update column (sum json-like data to get single float) for multiple columns
# cpu
# summed_runs = update_col(summed_runs, cpu_tot, ensemble_col)
# summed_runs = update_col(summed_runs, cpu_t1, ensemble_col)
# summed_runs = update_col(summed_runs, cpu_t2, ensemble_col)
# memory
# summed_runs = update_col(summed_runs, mem_tot, ensemble_col)
# summed_runs = update_col(summed_runs, mem_t1, ensemble_col)
# summed_runs = update_col(summed_runs, mem_t2, ensemble_col)
'''