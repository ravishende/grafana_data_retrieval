import pandas as pd
import re


# for current bp3d-worker naming convention ((bp3d-worker-k8s-...):
# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id(pod_name: str, helitack: bool = False) -> str:
    regex_pattern = r"bp3d-worker-k8s-([a-fA-F0-9]+)"
    # if we're looking for a helitack run, look for a different worker id
    if helitack:
        regex_pattern = r"bp3d-worker-helitack-k8s-([a-fA-F0-9]+)"
    match = re.search(regex_pattern, pod_name)
    worker_id = match.group(1) if match else None
    return worker_id


# for every worker pod in a given df, change pod's value to just be the worker id,
# drop all non-worker pods, then return that new, filtered dataframe
def filter_df_for_workers(dataframe: pd.DataFrame) -> pd.DataFrame:
    # run get_worker_id() on all pods to replace the pod with the ensemble or None if not a worker
    dataframe['Pod'] = dataframe['Pod'].apply(get_worker_id)
    # drop all the rows with non worker pods
    dataframe = dataframe.dropna(subset=["Pod"])
    return dataframe


# potentially useful functions:

# for old bp3d-worker naming convention (bp3d-worker-...):
# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id_old_ptrn(pod_name: str) -> str:
    old_regex_pattern = r"bp3d-worker-([a-fA-F0-9]+)-"
    match = re.search(old_regex_pattern, pod_name)
    worker_id = match.group(1) if match else None
    return worker_id


# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id_any_ptrn(pod_name: str) -> str:
    current_id = get_worker_id(pod_name)
    if current_id:
        return current_id

    old_id = get_worker_id_old_ptrn(pod_name)
    return old_id
