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
write_file = "csv_files/summed.csv" #calculated_training_data.csv


# given a json result_list return a float summation over all pods' values in that ensemble
def sum_pods_for_ensemble(result_list, ensemble):
    # handle if there is no ensemble id
    if not ensemble:
        return result_list

    # get str as json
    total = 0

    # loop over pods in ensemble, adding values to sum
    for item in result_list:
        pod = item["metric"]["pod"]
        worker_id = get_worker_id(pod)
        if worker_id is None:
            continue
        print("pod ensemble: ", worker_id, " ||  ensemble: ", ensemble)
        print("pod: ", pod, " ||  ensemble: ", UUID(worker_id), "\n")
        if str(UUID(worker_id)) == ensemble:
            value = item["value"][1]
            total += float(value)
    return total

# given a dataframe, title of a column to update, and title of the ensemble_id column,
# return a new dataframe with the edited column being float values instead of json data
# in every row where there was an ensemble id in the ensemble column
def update_col(df, update_col_title, ensemble_col_title):
    # change result_list strings to python lists
    df[update_col_title] = df[update_col_title].apply(literal_eval)

    # calculate totals summed over the ensemble for given column
    df[update_col_title] = df.apply(
        lambda row: sum_pods_for_ensemble(row[update_col_title], row[ensemble_col_title]) \
        if row[ensemble_col_title] else row[update_col_title],axis=1)

    return df[update_col_title]


# def get_first_30(pod_name):
#     if len(pod_name) >= 30:
#         return pod_name[:30]
#     return pod_name

# def get_ids(res_list):
#     ids = []
#     for item in res_list:
#         ids.append(get_worker_id(item['metric']['pod']))
#     return ids


# select columns to update (sum over)
ensemble_col = "ensemble_uuid"
cpu_tot_col = "cpu_total"
mem_tot_col = "mem_total"
# cpu_t1_col = "cpu_t1"
# mem_t1_col = "mem_t1"
# cpu_t2_col = "cpu_t2"
# mem_t2_col = "mem_t2"

# get the csv file as a pandas dataframe
summed_runs = pd.read_csv(read_file, index_col=0)
summed_runs[cpu_tot_col] = update_col(summed_runs, cpu_tot_col, ensemble_col)
summed_runs[mem_tot_col] = update_col(summed_runs, mem_tot_col, ensemble_col)

summed_runs.to_csv(write_file)
# summed_runs[mem_tot_col] = summed_runs[mem_tot_col].apply(literal_eval)
# summed_runs["mem_worker_ids"] = summed_runs[mem_tot_col].apply(get_ids)



# calculated_training_data = json_training_data.dropna(subset=ensemble_col)
# calculated_training_data[cpu_tot_col] = calculated_training_data[cpu_tot_col].apply(literal_eval)
# update columns - sum json to get single float
# calculated_training_data["worker_ids"] = calculated_training_data[cpu_tot_col].apply(get_ids)
# # calculated_training_data[cpu_tot_col] = update_col(calculated_training_data, cpu_tot_col, ensemble_col)
# calculated_training_data[mem_tot_col] = update_col(calculated_training_data, mem_tot_col, ensemble_col)
# calculated_training_data[cpu_t1_col] = update_col(calculated_training_data, cpu_t1_col, ensemble_col)
# calculated_training_data[mem_t1_col] = update_col(calculated_training_data, mem_t1_col, ensemble_col)
# calculated_training_data[cpu_t2_col] = update_col(calculated_training_data, cpu_t2_col, ensemble_col)
# calculated_training_data[mem_t2_col] = update_col(calculated_training_data, mem_t2_col, ensemble_col)

# print df and write to a new file
# print("na ensembles", calculated_training_data[ensemble_col].isna().sum())
subset = summed_runs[[cpu_tot_col, mem_tot_col, 'runtime']]
print(subset.head(1000))
# print(summed_runs[mem_tot_col].head(500))
# print("\n"*5)
# print(summed_runs["mem_worker_ids"].head(500))
# print("\n"*5)
# calculated_training_data.to_csv(write_file)



# info_data = json_training_data.get(["start","stop","runtime","cpu_usage"])
# info_data["cpu_usage"] = info_data["cpu_usage"].apply(literal_eval)
# info_data["id"] = info_data['cpu_usage'].apply(get_pods)

# def get_type_of_res_list(res_list):
#     for datapoint in res_list:
#         pod_name = datapoint['metric']['pod']
#         id_type = get_type_worker_pod(pod_name)
#         if id_type is None:
#             continue
#         return id_type

# def print_result(type, count, indices):
#     print('Number of  "' + type + '" id_type:', colored(count, "green"))
#     # print('\tIndices:\n\t', indices)

# info_data.to_csv("worker_templates.csv")



'''
def get_type_worker_pod(pod_name):
    worker_title = 'bp3d-worker-'
    suffix = 'k8s-'
    title_len = len(worker_title)
    suffix_len = len(suffix)
    if pod_name[0:title_len] == worker_title:
        if pod_name[title_len:title_len+suffix_len] == suffix:
            return "new"
        else:
            return "old"
    return None


info_data = json_training_data.get(["start","stop","runtime","cpu_usage"])

info_data["cpu_usage"] = info_data["cpu_usage"].apply(literal_eval)
info_data["id_type"] = info_data[cpu_total].apply(get_type_of_res_list)

old_count = info_data['id_type'].value_counts().get("old", 0)
old_indices = list(info_data[info_data['id_type'] == "old"].index)

new_count = info_data['id_type'].value_counts().get("new", 0)
new_indices = list(info_data[info_data['id_type'] == "new"].index)

none_count = info_data['id_type'].isna().sum()
none_indices = list(info_data[info_data['id_type'].isna()].index)

print_result("None", none_count, none_indices)
print_result("new", new_count, new_indices)
print_result("old", old_count, old_indices)
'''
