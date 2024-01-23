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
write_file = "csv_files/partial_summed_all_metrics.csv"


# given a list of metrics, return a new list that has _total, _t1, and _t2 appended to each metric
# to create a new list that is 3 times the size of metrics_list
def get_columns_from_metrics(metric_list):
    summary_columns = []
    for name in metric_list:
        summary_columns.append(name + "_total")
        summary_columns.append(name + "_t1")
        summary_columns.append(name + "_t2")
    return summary_columns


# insert percent columns into a dataframe and return the updated dataframe
# parameters:
    # df: a dataframe with columns that have a _total, _t1, and _t2 appended for each metric in the following metric lists
    # percent_metrics: a list of names of metrics that will become the percent columns after appending _total, _t1, and _t2 to it
    # numerator_metrics: a list of metrics that are are the base (will add the _total, _t1, _t2), for the columns that will be used as the numerator for the percent operation
    # denominator_metrics: a list of static metrics that the columns that will be used as the numerator for the percent operation
        # NOTE: percent_metrics, numerator_metrics, denominator_metrics must all be the same length, with each index corresponding to each other
# returns:
    # an updated df with inserted percent columns
        # percent columns are formed by: 
        # get a columns list from each of the three input metric lists (by adding _total, _t1, and _t2 to each metric name)
        # insert the percent columns into the dataframe as 100 * numerator columns / denominator columns
def insert_percent_cols(df, percent_metrics, numerator_metrics, denominator_metrics):
    # make sure that all metrics lists are the same size
    if not (len(percent_metrics) == len(numerator_metrics) == len(denominator_metrics)):
        raise ValueError("percent_metrics, numerator_metrics, denominator_metrics must all be the same length")

    # get columns lists by adding _total, _t1, and _t2 to each metric in each metric_list in percent_metrics_format
    percent_cols = get_columns_from_metrics(percent_metrics)
    numerator_cols = get_columns_from_metrics(numerator_metrics)
    denominator_cols = []
    # check if denominator metrics are the static metrics, since the use case often is
    if denominator_metrics == STATIC_METRICS:
        # since denominator cols are for static metrics, they don't need to be 
        denominator_cols = [metric for metric in denominator_metrics for _ in range(3)]  # copy metrics 3 times each to account for added _total, _t1, _t2 in non static metric lists
    else:
        denominator_cols = get_columns_from_metrics(denominator_metrics)

    # calculate percentage columns
    for i in range(len(percent_cols)):
        df[percent_cols[i]] = 100 * df[numerator_cols[i]] / df[denominator_cols[i]]

    return df


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


def fill_in_static_na(df, static_metrics):
    # Group df by 'ensemble_uuid'
    ensemble_groups = df.groupby('ensemble_uuid')

    # Create a subset of df with rows where any static metric is NA
    na_static_df = df[df[static_metrics].isna().any(axis=1)]

    # Iterate over the rows in the subset
    for i, row in na_static_df.iterrows():
        ensemble_uuid = row['ensemble_uuid']

        # For each static metric, try to find a non-NA value from the same ensemble
        for metric in static_metrics:
            # if the row's metric is not na, move on
            if not pd.isna(row[metric]):
                continue

            # Get all rows with the same ensemble_uuid
            ensemble_rows = ensemble_groups.get_group(ensemble_uuid)

            # find the first row in the ensemble that doesn't have NA for this metric
            valid_rows = ensemble_rows[ensemble_rows[metric].notna()].iloc[0]

            if not valid_rows.empty:
                # get the metric's value for the valid row
                valid_value = valid_rows[metric]
                na_static_df.at[i, metric] = valid_value

    # Update the original df with filled values
    df.update(na_static_df)

    return df


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

STATIC_METRICS = [
    "cpu_request",
    "mem_request",
]



# get a list of metrics that need to be summed (all metrics - static metrics)
metrics_to_sum = [metric for metric in all_metrics if metric not in STATIC_METRICS]
# get names of all columns to sum
columns_to_sum = get_columns_from_metrics(metrics_to_sum)


# get the csv file as a pandas dataframe
summed_runs = pd.read_csv(read_file, index_col=0)
# specify the ensemble ids column name
ensemble_col = "ensemble_uuid"

# update columns to get float values from json
print_heading("Summing Up Columns")
summed_runs = update_columns(summed_runs, columns_to_sum, ensemble_col, static=False)
print_heading("Getting Values for Static Columns")
summed_runs = update_columns(summed_runs, STATIC_METRICS, ensemble_col, static=True)

# try to fill in any na values in static columns by looking at other runs with same ensemble
summed_runs = fill_in_static_na(summed_runs, STATIC_METRICS)

# insert percent columns into the dataframe
percent_metrics = ["cpu_request_%", "mem_request_%"]  # these do not exist yet - the columns for these metrics will be calculated
numerator_metrics = ["cpu_usage", "mem_usage"]
denominator_metrics = ["cpu_request", "mem_request"]
summed_runs = insert_percent_cols(summed_runs, percent_metrics, numerator_metrics, denominator_metrics)

# save the dataframe to a file and print it
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