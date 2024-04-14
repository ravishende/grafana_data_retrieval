from multiprocessing.sharedctypes import Value
import pandas as pd
from ast import literal_eval
from termcolor import colored
from uuid import UUID
import shutil
import sys
import os
# get set up to be able to import helper files from parent directory (grafana_data_retrieval)
sys.path.append("../../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
grandparent = os.path.dirname(parent)
sys.path.append(grandparent)
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

# given a list of metrics, return a new list that has _total, _t1, and _t2 appended to each metric
# to create a new list that is 3 times the size of metrics_list
def get_columns_from_metrics(metric_list, num_inserted_duration_cols=3, include_total=True):
    summary_columns = []
    for name in metric_list:
        # get total duration column names
        if include_total:
            summary_columns.append(name + "_total")
        # get duration_t_i column names - append num_inserted_duraion_cols added to the name
        for i in range(1, num_inserted_duration_cols+1):
            summary_columns.append(name + "_t" + str(i))
    return summary_columns


# insert percent columns into a dataframe and return the updated dataframe
# parameters:
    # df: a dataframe with columns that have a _total, _t1, and _t2 appended for each metric in the following metric lists
    # percent_metrics: a list of names of metrics that will become the percent columns after appending _total, _t1, and _t2 to it
    # numerator_metrics: a list of metrics that are are the base (will add the _total, _t1, _t2), for the columns that will be used as the numerator for the percent operation
    # denominator_metrics: a list of no_sum metrics that the columns that will be used as the numerator for the percent operation
        # Note: percent_metrics, numerator_metrics, denominator_metrics must all be the same length, with each index corresponding to each other
# returns:
    # an updated df with inserted percent columns
        # percent columns are formed by: 
        # get a columns list from each of the three input metric lists (by adding _total, _t1, and _t2 to each metric name)
        # insert the percent columns into the dataframe as 100 * numerator columns / denominator columns
def insert_percent_cols(df, percent_metrics, numerator_metrics, denominator_metrics, static_metrics, num_inserted_duration_cols=3):
    # make sure that all metrics lists are the same size
    if not (len(percent_metrics) == len(numerator_metrics) == len(denominator_metrics)):
        raise ValueError("percent_metrics, numerator_metrics, denominator_metrics must all be the same length")
    
    # make sure num_duration_cols isn't out of bounds and duration columns are named correctly.
    final_duration_col = df[f"duration_t{num_inserted_duration_cols}"]
    assert(isinstance(final_duration_col[0], int) or isinstance(final_duration_col[0], float))
        
    # get columns lists by adding _t1, _t2, etc. to each metric in each metric_list in percent_metrics_format
    percent_col_names = get_columns_from_metrics(percent_metrics, num_inserted_duration_cols, include_total=False)
    numerator_col_names = get_columns_from_metrics(numerator_metrics, num_inserted_duration_cols, include_total=False)
    
    denominator_col_names = []
    # handle if any denominator metrics are static metrics. 
    for metric in denominator_metrics:
        if metric in static_metrics:
            # If they are, don't add _t_i to the end of them and just repeat them num_inserted_duration_cols times
            denominator_col_names += [metric]*num_inserted_duration_cols
        else:
            # get column names for the one metric
            denominator_col_names += get_columns_from_metrics([metric], num_inserted_duration_cols, include_total=False)

    # calculate percentage columns
    for i in range(len(percent_col_names)):
        # get numerator and denominator for calculation
        numerator_col = df[numerator_col_names[i]]
        denominator_col = df[denominator_col_names[i]]
        percent_col = percent_col_names[i]
        # handle if denominator column is cpu_request (multiply it by duration_t{x} to get into cpu_seconds)
        if denominator_col_names[i] == "cpu_request":
            time_suffix = numerator_col_names[i].split('_')[-1]  # should be "total", "t1", "t2", etc. 
            if time_suffix == "total":
                continue
            duration_t_x = df[f"duration_{time_suffix}"]
            denominator_col = denominator_col * duration_t_x
        # calculate and insert percent column
        df[percent_col] = 100 * numerator_col / denominator_col

    return df


# Given a json result_list (json-like data) of a queried metric (cpu or mem usage),
# Return:
    # a float summation over all pods' values in that ensemble (if all conditions are met)
        # or -1 if no pods match ensemble_id
    # result_list (if there is no ensemble id provided)
    # None (if there are no bp3d-workers to sum over in result_list)
def sum_pods_for_ensemble(result_list, ensemble, sum=True):
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
            if not sum:
                return total

        # if there are no worker pods that match the ensemble id, return -1
        if total == 0 and not worker_found:
            total = -1

    return total


def fill_in_static_na(df, static_metrics):
    # Group df by 'ensemble_uuid'
    ensemble_groups = df.groupby('ensemble_uuid')

    # Create a subset of df with rows where any no-sum metric is NA
    na_no_sum_df = df[df[static_metrics].isna().any(axis=1)]

    # Iterate over the rows in the subset
    for i, row in na_no_sum_df.iterrows():
        ensemble_uuid = row['ensemble_uuid']

        # For each no-sum metric, try to find a non-NA value from the same ensemble
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
                na_no_sum_df.at[i, metric] = valid_value

    # Update the original df with filled values
    df.update(na_no_sum_df)

    return df


# given a dataframe, title of a column to update, and title of the ensemble_id column,
# return a new dataframe with the edited column being float values instead of json data
# in every row where there was an ensemble id in the ensemble column, or None if there are
# no bp3d-worker-pods
def update_col(df, update_col_title, ensemble_col_title, sum=True):
    # drop na values of column
    df = df.dropna(subset=update_col_title)

    # get result_list as list (not string)
    df[update_col_title] = df[update_col_title].apply(literal_eval)

    # calculate totals summed over the ensemble for given column
    df[update_col_title] = df.apply(
        lambda row: sum_pods_for_ensemble(row[update_col_title], row[ensemble_col_title], sum=sum) \
        if row[ensemble_col_title] else row[update_col_title], axis=1)

    return df

# given a dataframe, list of column names to update, and title of the ensemble_id column,
# return a new dataframe with the edited columns being float values of the summed over json data
def update_columns(df, update_col_names, ensemble_col_title, no_sum_metrics):
    num_update_cols = len(update_col_names)
    for i, col_name in enumerate(update_col_names):
        print(f"\n\nUpdating {col_name}", colored(f"({i+1}/{num_update_cols})", "green"))
        # set sum to False if it is a no_sum column, otherwise, set sum to True
        if col_name in no_sum_metrics:
            sum = False
        else:
            sum = True
        # update the column (summing or not depending on sum)
        df = update_col(df, col_name, ensemble_col_title, sum=sum)
    return df



'''
============================================
                Main Program
============================================
'''

if __name__ == "__main__":
    read_file = "old/csv_files/queried.csv" 
    write_file = "old/csv_files/partial_summed_all_metrics.csv"

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
        "mem_request"
        "cpu_request"
    ]

    # get a list of metrics that need to be summed (all metrics - no_sum metrics)
    non_static_metrics = [metric for metric in all_metrics if metric not in static_metrics]
    # get names of all columns to sum
    columns_to_sum = get_columns_from_metrics(non_static_metrics)
    all_metric_cols = static_metrics + columns_to_sum

    # get the csv file as a pandas dataframe
    summed_runs = pd.read_csv(read_file, index_col=0)
    # specify the ensemble ids column name
    ensemble_col = "ensemble_uuid"

    # update columns to get float values from json
    print_heading("Summing Up Columns")
    summed_runs = update_columns(summed_runs, all_metric_cols, ensemble_col, no_sum_metrics=static_metrics)

    # try to fill in any na values in static columns by looking at other runs with same ensemble
    summed_runs = fill_in_static_na(summed_runs, static_metrics)

    # insert percent columns into the dataframe
    percent_metrics = ["cpu_request_%", "mem_request_%"]  # these do not exist yet - the columns for these metrics will be calculated
    numerator_metrics = ["cpu_usage", "mem_usage"]
    denominator_metrics = static_metrics
    summed_runs = insert_percent_cols(summed_runs, percent_metrics, numerator_metrics, denominator_metrics, static_metrics)

    # save the dataframe to a file and print it
    summed_runs.to_csv(write_file)
    print(summed_runs)


'''
# split the summed_runs into runs that had data for total resources and for ones that didn't (no bp3d-workers)
# in other words, if cpu_tot or mem_tot are none, add row to na_mask
# na_mask = summed_runs[cpu_tot].isna() | summed_runs[mem_tot].isna()
# na_worker_runs = summed_runs[na_mask]
# valid_worker_runs = summed_runs[~na_mask]

# summed_runs = update_col(summed_runs, "cpu_request_total", ensemble_col, sum=False)
# summed_runs = update_col(summed_runs, "mem_request_total", ensemble_col, sum=False)
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