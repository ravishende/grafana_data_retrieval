import re
# gets the worker id for a given pod or returns None if it is not a bp3d-worker
# def get_worker_id(pod_name):
#     worker_title = 'bp3d-worker-'
#     suffix = 'k8s-'
    
#     # if pod_name is a bp3d worker, return the worker id
#     title_len = len(worker_title)
#     suffix_len = len(suffix)
#     if pod_name[0:title_len] == worker_title:
#         # there are two types of bp3d worker pods. Some starting with 'bp3d-worker-', some with 'bp3d-worker-k8s-'.
#         # collect the id for either type
#         stripped_string = ""
#         if pod_name[title_len:title_len+suffix_len] == suffix:
#             stripped_string = pod_name[title_len+suffix_len:]
#         else:
#             stripped_string = pod_name[title_len:]

#         # pods have following format: bp3d-worker-k8s-{UUID}-{jobIndex}-{k8s-id}
#         # only select the UUID (first section after the prefix)
#         ensemble_id =  stripped_string.split("-")[0]
#         return ensemble_id
    
#     # if it isn't a worker pod, return None
#     return None

# for current bp3d-worker naming convention:
# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id(pod_name):
    regex_pattern = r"bp3d-worker-.*?-k8s-([a-fA-F0-9]+)"
    match = re.search(regex_pattern, pod_name)
    worker_id = match.group(1) if match else None
    return worker_id

# for old bp3d-worker naming convention:
# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id_old_ptrn(pod_name):
    old_regex_pattern = r"bp3d-worker-([a-fA-F0-9]+)-"
    match = re.search(old_regex_pattern, pod_name)
    worker_id = match.group(1) if match else None
    return worker_id

# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id_any_ptrn(pod_name):
    old_id = get_worker_id_old_ptrn(pod_name)
    if old_id is not None:
        return old_id

    current_id = get_worker_id(pod_name)
    return current_id



# for every worker pod in a given df, change pod's value to just be the worker id,
# drop all non-worker pods, then return that new, filtered dataframe
def filter_df_for_workers(dataframe):
    # run get_worker_id() on all pods to replace the pod with the ensemble or None if not a worker
    dataframe['Pod'] = dataframe['Pod'].apply(get_worker_id)
    # drop all the rows with non worker pods
    dataframe = dataframe.dropna(subset=["Pod"])
    return dataframe