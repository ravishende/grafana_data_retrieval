# gets the worker id for a given pod or returns None if it is not a bp3d-worker
def get_worker_id(pod_name):
    worker_title = 'bp3d-worker-'
    suffix = 'k8s-'
    
    # if pod_name is a bp3d worker, return the worker id
    title_len = len(worker_title)
    suffix_len = len(suffix)
    if pod_name[0:title_len] == worker_title:
        # there are two types of bp3d worker pods. Some starting with 'bp3d-worker-', some with 'bp3d-worker-k8s-'.
        # collect the id for either type
        if pod_name[title_len:title_len+suffix_len] == suffix:
            return pod_name[title_len+suffix_len:-1]
        else:
            return pod_name[title_len:-1]
    
    # if it isn't a worker pod, return None
    return None


# for every worker pod in a given df, change pod's value to just be the worker id,
# drop all non-worker pods, then return that new, filtered dataframe
def filter_df_for_workers(dataframe):
    # run get_worker_id() on all pods to replace the pod with the ensemble or None if not a worker
    dataframe['Pod'] = dataframe['Pod'].apply(get_worker_id)
    # drop all the rows with non worker pods
    dataframe = dataframe.dropna(subset=["Pod"])
    return dataframe