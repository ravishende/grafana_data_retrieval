import pandas as pd 
import json
from ast import literal_eval
from termcolor import colored
from uuid import UUID
import sys
import os
# get set up to be able to import helper files from parent directory (grafana_data_retrieval)
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("node_metrics_retrieval.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.filtering import get_worker_id
'''
NOTE:
This file should be run after find_ensembles.py creates a new csv 
of the training data with an included ensemble_id column.
This file assumes there is already a column called 'ensemble'
that contains the ensemble that each run is a part of.
'''

# settings
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
read_file = "csv_files/queried_w_ids.csv" #p2_ensemble_included.csv
total_write_file = "csv_files/summed.csv"
success_write_file = "csv_files/summed_success.csv" #calculated_training_data.csv
na_write_file = "csv_files/summed_na.csv"



# Given a json result_list (json-like data) of a queried metric (cpu or mem usage),
# Return:
    # a float summation over all pods' values in that ensemble (if all conditions are met)
    # result_list (if there is no ensemnble id provided)
    # None (if there are no bp3d-workers to sum over in result_list)
def sum_pods_for_ensemble(result_list, ensemble):
    # handle if there is no ensemble id
    if not ensemble:
        return result_list

    total = 0
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

        # if there are no bp3d-worker-pods, return None
        if total == 0:
            return None

    return total

# given a dataframe, title of a column to update, and title of the ensemble_id column,
# return a new dataframe with the edited column being float values instead of json data
# in every row where there was an ensemble id in the ensemble column, or None if there are
# no bp3d-worker-pods
def update_col(df, update_col_title, ensemble_col_title):
    # change result_list strings to python lists
    df[update_col_title] = df[update_col_title].apply(literal_eval)

    # calculate totals summed over the ensemble for given column
    df[update_col_title] = df.apply(
        lambda row: sum_pods_for_ensemble(row[update_col_title], row[ensemble_col_title]) \
        if row[ensemble_col_title] else row[update_col_title],axis=1)

    return df[update_col_title]



'''
============================================
                Main Program
============================================
'''


# select columns to update (sum over) as well as ensemble ids column
ensemble_col = "ensemble_uuid"
cpu_tot_col = "cpu_total"
mem_tot_col = "mem_total"
# cpu_t1_col = "cpu_t1"
# mem_t1_col = "mem_t1"
# cpu_t2_col = "cpu_t2"
# mem_t2_col = "mem_t2"

# get the csv file as a pandas dataframe
summed_runs = pd.read_csv(read_file, index_col=0)

# update columns (sum json-like data to get single float)
summed_runs[cpu_tot_col] = update_col(summed_runs, cpu_tot_col, ensemble_col)
summed_runs[mem_tot_col] = update_col(summed_runs, mem_tot_col, ensemble_col)
# summed_runs[cpu_t1] = update_col(summed_runs, cpu_t1, ensemble_col)
# summed_runs[mem_t1] = update_col(summed_runs, mem_t1, ensemble_col)
# summed_runs[cpu_t2] = update_col(summed_runs, cpu_t2, ensemble_col)
# summed_runs[mem_t2] = update_col(summed_runs, mem_t2, ensemble_col)

# split the summed_runs into runs that had data for floats and for ones that didn't (no bp3d-workers)
na_mask = summed_runs['cpu_total'].isna() | summed_runs['mem_total'].isna()
na_worker_runs = summed_runs[na_mask]
valid_worker_runs = summed_runs[~na_mask]

# save dataframes to new files and print summed_runs
summed_runs.to_csv(total_write_file)
valid_worker_runs.to_csv(success_write_file)
na_worker_runs.to_csv(na_write_file)

print(summed_runs)

# can be used to find the worker ids of each run for analysis/debugging purposes
# def get_ids(res_list):
#     ids = []
#     for item in res_list:
#         ids.append(get_worker_id(item['metric']['pod']))
#     return ids
