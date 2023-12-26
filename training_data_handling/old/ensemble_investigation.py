import pandas as pd
from ast import literal_eval
from termcolor import colored
import sys
import os
# get set up to be able to import files from parent directory (grafana_data_retrieval)
# utils.py and inputs.py not in this current directory and instead in the parent
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("node_metrics_retrieval.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.filtering import get_worker_id


# settings
pd.set_option("display.max_columns", None)
csv_file = "csv_files" #p2_ensemble_included.csv

'''
This file is used to find if there are any matches between the bp3d pods' worker_ids 
during a run and the run's ensemble id.
'''



def find_all_ensembles(result_list):
    ensembles = {}

    # for each pod, if its a bp3d worker, increment its worker_id count in ensembles
    for datapoint in result_list:
        # get worker_id of current pod
        pod = datapoint['metric']['pod']
        worker_id = get_worker_id(pod)
        
        # check if pod was a bp3d worker
        if worker_id is None: continue

        # increment count of worker_id in ensembles dictionary
        if worker_id in ensembles:
            ensembles[worker_id] += 1
        else:
            ensembles[worker_id] = 1

    # if there are worker pods, return the most common ensemble
    if len(ensembles) > 0:
        # return max(ensembles, key=ensembles.get)
        return ensembles.keys()

    # if there are no ensembles, return None
    return None

def key_in_ensemble(ensemble, worker_ids):
    if worker_ids is None: return None
    for w_id in worker_ids:
        if w_id in ensemble:
            return w_id
    return None


df = pd.read_csv(csv_file, index_col=0)

# drop all rows without an ensemble id
ensemble_df = df.dropna(subset=['ensemble'])
ensemble_df = ensemble_df.reset_index(drop=True)

# insert a column 'worker_ids' into ensemble_df that has all the bp3d worker_ids during the run
ensemble_df['cpu_usage'] = ensemble_df['cpu_usage'].apply(literal_eval)
ensemble_df['worker_ids'] = ensemble_df['cpu_usage'].apply(find_all_ensembles)

# create a new column "match" that is the worker_id if there is a worker_id in row['worker_ids'] that is contained within row['ensemble']. Otherwise, row['match'] is None
ensemble_df['match'] = ensemble_df.apply(lambda row: key_in_ensemble(row['ensemble'], row['worker_ids']), axis=1)
# drop all rows where 'match' is None
print("\n"*2, colored("ensemble df:", "green"))
print(ensemble_df, "\n"*5)
ensemble_df = ensemble_df.dropna(subset=['match'])

# print the final df. If it is empty, there are no matches between worker_ids and ensemble_ids
print(colored("ensemble df after dropping NA matches:", "green"))
print(ensemble_df, "\n")
