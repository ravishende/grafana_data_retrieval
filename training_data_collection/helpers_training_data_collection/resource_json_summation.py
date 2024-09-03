'''
This file assumes there is already a column called 'ensemble'
that contains the ensemble that each run is a part of.
'''
# autopep8: off
from ast import literal_eval
from uuid import UUID
import shutil
import sys
import os
import pandas as pd
from termcolor import colored
from metrics_and_columns_setup import GET_DURATION_COLS
from workflow_files import PHASE_1_FILES
# get set up to be able to import helper files from parent directory (grafana_data_retrieval)
sys.path.append("../../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
grandparent = os.path.dirname(parent)
sys.path.append(grandparent)
# pylint: disable=wrong-import-position
from helpers.filtering import get_worker_id
from helpers.printing import print_heading
# autopep8: off

ENSEMBLE_RUN_FILE = PHASE_1_FILES['read']
IS_HELITACK = False # runs that are done through the dev that have non default hardware settings

# settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

def get_num_inserted_duration_cols():
    return GET_DURATION_COLS()['num_cols']

def set_helitack_status(helitack_status=False):
    if helitack_status != True and helitack_status != False:
        raise ValueError("helitack_status must be either True or False, with False by default")
    # update the helitack status
    # pylint: disable=global-statement
    global IS_HELITACK
    IS_HELITACK = helitack_status

# given a list of metrics, return a new list that has _total, _t1, and _t2 appended to each metric
# to create a new list that is 3 times the size of metrics_list
def get_columns_from_metrics(
    metric_list, num_inserted_duration_cols=None, include_total=True):
    if num_inserted_duration_cols is None:
        num_inserted_duration_cols = get_num_inserted_duration_cols()
    summary_columns = []
    for name in metric_list:
        # get total duration column names
        if include_total:
            summary_columns.append(name + "_total")
        # get duration_t_i column names - append num_inserted_duraion_cols added to the name
        if num_inserted_duration_cols > 0:
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
def insert_percent_cols(df, percent_metrics, numerator_metrics, denominator_metrics, static_metrics, num_inserted_duration_cols=None):
    if num_inserted_duration_cols is None:
        num_inserted_duration_cols = get_num_inserted_duration_cols()

    # make sure that all metrics lists are the same size
    if not (len(percent_metrics) == len(numerator_metrics) == len(denominator_metrics)):
        raise ValueError("percent_metrics, numerator_metrics, denominator_metrics must all be the same length")

    # make sure num_duration_cols isn't out of bounds and duration columns are named correctly.
    if num_inserted_duration_cols > 0:
        final_duration_col = df[f"duration_t{num_inserted_duration_cols}"]
        try:
            final_duration_col = final_duration_col.astype(int)
        except Exception as exc:
            raise ValueError(f"Expected final duration column to be numeric, but was type {type(final_duration_col[0])}.") from exc

    # get columns lists by adding _total, and _t1, _t2, etc. to each metric in each metric_list in percent_metrics_format. If num_inserted_duration_cols==0, it will just be _total metrics
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
    zipped_col_names = zip(percent_col_names, numerator_col_names, denominator_col_names)
    for percent_name, numerator_name, denominator_name in zipped_col_names:
        # denominator col may be updated - save it separately to avoid changing the column in the df
        denominator_col = df[denominator_name]
        # if denominator is cpu_request, multiply it by duration_t{x} to get into cpu_seconds so
        # it is the same unit as the numerator (cpu_usage)
        if denominator_name == "cpu_request":
            time_suffix = numerator_name.split('_')[-1]  # should be "total", "t1", "t2", etc. 
            if time_suffix == "total":
                continue
            # multiply cpu_request denominator col by duration_t{x} to get into cpu_seconds
            duration_t_x = df[f"duration_{time_suffix}"]
            denominator_col = denominator_col * duration_t_x

        # calculate and insert percent column
        df[percent_name] = 100 * df[numerator_name] / denominator_col

    return df


#TODO: CLEAN UP MESSY HELITACK FIX
def remove_dashes(input_string):
    return input_string.replace('-', '')

def truncate_str(uuid_string):
    return uuid_string[:-2]


# Given a json result_list (json-like data) of a queried metric (cpu or mem usage),
# Return:
    # a float summation over all pods' values in that ensemble (if all conditions are met)
        # or -1 if no pods match ensemble_id
    # result_list (if there is no ensemble id provided)
    # None (if there are no bp3d-workers to sum over in result_list)
def sum_pods_for_ensemble(result_list, ensemble, sum=True):
    ens_series = pd.read_csv(ENSEMBLE_RUN_FILE)['ensemble_uuid']
    ens_series = ens_series.apply(remove_dashes)
    truncated_ens_series = ens_series.apply(truncate_str)
    # handle if there is no ensemble id
    if not ensemble:
        return result_list

    total = 0
    worker_found = False
    # loop over pods in ensemble, adding values to sum
    for item in result_list:
        # get worker id of each pod
        pod = item["metric"]["pod"]
        worker_id = get_worker_id(pod, helitack=IS_HELITACK)

        # skip pod if it's not a bp3d-worker-pod
        if worker_id is None:
            continue
        # get the uuid from the string
        UUID_NO_DASHES_LEN = 36
        TRUNCATED_ENS_LEN = 30
        uuid = None
        if len(worker_id) == UUID_NO_DASHES_LEN:
            uuid = UUID(worker_id)
        elif len(worker_id) == TRUNCATED_ENS_LEN:
            try:
                truncated_ens_index = truncated_ens_series[truncated_ens_series == worker_id].index[0]
            except IndexError:
                continue
            full_ens = ens_series.iloc[truncated_ens_index]
            uuid = UUID(full_ens)
        else:
            try:
                uuid = UUID(worker_id)
            # pylint: disable=bare-except
            except:
                continue
        # if ensemble id matches run's ensemble, add it to total
        if str(uuid) == ensemble:
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



# ============================================
#                 Main Program
# ============================================

if __name__ == "__main__":
    READ_FILE = "old/csv_files/queried.csv"
    WRITE_FILE = "old/csv_files/partial_summed_all_metrics.csv"

    # all queried metrics
    ALL_METRICS = [
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
        "mem_request"
        "cpu_request"
    ]

    # get a list of metrics that need to be summed (all metrics - no_sum metrics)
    NON_STATIC_METRICS = [metric for metric in ALL_METRICS if metric not in STATIC_METRICS]
    # get names of all columns to sum
    columns_to_sum = get_columns_from_metrics(NON_STATIC_METRICS)
    all_metric_cols = STATIC_METRICS + columns_to_sum

    # get the csv file as a pandas dataframe
    summed_runs = pd.read_csv(READ_FILE, index_col=0)
    # specify the ensemble ids column name
    ensemble_col = "ensemble_uuid"

    # update columns to get float values from json
    print_heading("Summing Up Columns")
    summed_runs = update_columns(summed_runs, all_metric_cols, ensemble_col, no_sum_metrics=STATIC_METRICS)

    # try to fill in any na values in static columns by looking at other runs with same ensemble
    summed_runs = fill_in_static_na(summed_runs, STATIC_METRICS)

    # insert percent columns into the dataframe
    PERCENT_METRICS = ["cpu_request_%", "mem_request_%"]  # these do not exist yet - the columns for these metrics will be calculated
    NUMERATOR_METRICS = ["cpu_usage", "mem_usage"]
    DENOMINATOR_METRICS = STATIC_METRICS
    summed_runs = insert_percent_cols(summed_runs, PERCENT_METRICS, NUMERATOR_METRICS, DENOMINATOR_METRICS, STATIC_METRICS)

    # save the dataframe to a file and print it
    summed_runs.to_csv(WRITE_FILE)
    print(summed_runs)
