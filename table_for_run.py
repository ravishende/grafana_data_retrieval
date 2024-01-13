import pandas as pd
import shutil
from pprint import pprint
from datetime import timedelta
from termcolor import colored
# modules
from tables import Tables
from helpers.printing import print_dataframe_dict
from helpers.querying import query_data
from helpers.time_functions import delta_to_time_str, time_str_to_delta, datetime_ify, calculate_offset


# settings
read_file = "training_data_handling/csv_files/non_zeros.csv"
NAMESPACE = 'wifire-quicfire'

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)
pd.set_option('display.max_colwidth', 30)
pd.set_option("display.max_rows", None)



'''
Categories of metrics for querying: 
    1. Max Over Time Range Metrics
        - Metrics that fluctuate over a run, but only the max value matters (e.g. Memory Usage)
    2. Increasing Metrics
        - metrics that increase over a run (e.g. CPU Usage)
    3. Static Metrics
        - metrics that never change throughout a run (e.g. Memory Limits)
'''



'''
========================================
            Helper functions
========================================
'''
# assemble queries for all max based metrics
def assemble_max_queries(max_query_bodies, start, duration_seconds):
    # get offset and duration for query
    offset = calculate_offset(start, duration_seconds)
    duration = delta_to_time_str(timedelta(seconds=duration_seconds))

    # get components of query ready to be assembled
    prefix = "sum by (node, pod) (max_over_time("
    suffix = '{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'

    # assemble queries
    max_queries = {}
    for title, query_body in max_query_bodies.items():
        max_queries[title] = prefix + query_body + suffix

    return max_queries


# assemble queries for all increase based metrics
def assemble_increase_queries(increase_query_bodies, start, duration_seconds):
    # get offset and duration for query
    offset = calculate_offset(start, duration_seconds)
    duration = delta_to_time_str(timedelta(seconds=duration_seconds))

    # get components of query ready to be assembled
    prefix = "sum by (node, pod) (increase("
    suffix = '{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'

    # assemble queries
    increase_queries = {}
    for title, query_body in increase_query_bodies.items():
        increase_queries[title] = prefix + query_body + suffix

    return increase_queries


# assemble queries for all static metrics
def assemble_static_queries(static_query_bodies, start, duration_seconds):
    # get offset for query 5 seconds after run started instead of as run ends - fewer NA pods for limits and requests
    # to get offset for when the run ends, use `offset = calculate_offset(start, duration_seconds)`
    offset = calculate_offset(start, 5)

    # get prefix of query ready for assembly (suffix gets defined while looping over query_bodies)
    prefix = "sum by (node, pod) ("

    # get the right suffix depending on the resource
    suffixes = {
         # set resource to cpu and multiply by duration seconds to get metric from measuring cpu cores to cpu seconds
         # ex: cpu_usage is measured in cpu seconds, so to make the cpu requests % (cpu_usage/cpu_requests) accurate, they both have to be in the same units.
        'cpu':'{resource="cpu", namespace="' + NAMESPACE + '"} offset ' + str(offset) + ') * ' + str(duration_seconds),
        # set resource to memory. It is already in the preferred unit of bytes
        'mem':'{resource="memory", namespace="' + NAMESPACE + '"} offset ' + str(offset) + ')'
    }

    # assemble queries
    static_queries = {}
    for title, query_body in static_query_bodies.items():
        # get the resource and corresponding suffix
        resource = title[:3].lower()
        suffix = suffixes[resource]
        # assemble query and put into static_queries
        static_queries[title] = prefix + query_body + suffix
        
    return static_queries

# given a dict of all query bodies, a dict of all metrics, a run start (datetime), and run duration_seconds (int or float)
# return two dictionaries, one with full queries, and one with partial queries, both to be passed into tables_class methods
def get_queries(query_bodies_dict, metrics_dict, start, duration_seconds):
    # assemble queries
    max_queries = assemble_max_queries(query_bodies_dict['max'], start, duration_seconds)
    increase_queries = assemble_increase_queries(query_bodies_dict['increase'], start, duration_seconds)
    static_queries = assemble_static_queries(query_bodies_dict['static'], start, duration_seconds)

    # get all queries into one dictionary
    unsorted_queries = {}
    unsorted_queries.update(max_queries)
    unsorted_queries.update(increase_queries)
    unsorted_queries.update(static_queries)

    # assemble queries and partial_queries for the run - sorted versions of the unsorted_queries separated by full queries vs partial_queries
    run_queries = {key: unsorted_queries[key] for key in metrics}
    run_partial_queries = {key: unsorted_queries[key] for key in partial_metrics}

    return run_queries, run_partial_queries


# given a table dataframe of either cpu quota or memory quota, a resource ("mem" or "cpu"),
# fill in the na values for requests, limits, requests %, and limits %
def fill_in_static_na(df, resource):
    # use resource to determine resource_str
    resource_str = ""
    if resource == "cpu":
        resource_str = "CPU"
    elif resource == "mem":
        resource_str = "Memory"
    else:
        raise ValueError('resource must be either "cpu" or "mem"')
    
    # fill in na values
    df[[f'{resource_str} Requests', f'{resource_str} Limits']] = df[[f'{resource_str} Requests', f'{resource_str} Limits']].fillna(method='ffill')
    df[f'{resource_str} Requests %'] = df[f'{resource_str} Usage'].astype(float) / df[f'{resource_str} Requests'].astype(float) * 100 
    df[f'{resource_str} Limits %'] = df[f'{resource_str} Usage'].astype(float) / df[f'{resource_str} Limits'].astype(float) * 100

    return df


# Returns a dataframe with changed names of certain columns to represent the true data
# for a run rather than the current snapshot in time.
# Note: The updated queries already query the correct information, we just have to change
# the names to match, after we do the querying from the tables_class methods (which rely on the names).
def rename_df_metrics(df):
    rename_dict = {}
    for old_name in df.columns:
        if "IOPS" in old_name: # change "IOPS" column names to "IO"
            rename_dict[old_name] = old_name.replace("IOPS", "IO")
        elif "Current" in old_name: # get rid of "Current"
            rename_dict[old_name] = old_name.replace("Current", "")
        elif "Rate of" in old_name: # get rid of "Rate of"
            rename_dict[old_name] = old_name.replace("Rate of", "")
        else: # keep old name
            rename_dict[old_name] = old_name

    renamed_df = df.rename(columns=rename_dict)
    return renamed_df

# if tables is a single dataframe or list of dataframes, updates names of the metrics in tables.
# if tables is a dict of dataframes, updates names of the tables and their metrics.
# returns the orginal dataframe or dict of dataframes
def rename_tables(tables):
    # tables is a single dataframe
    if isinstance(tables, pd.DataFrame):
        return rename_df_metrics(tables)

    # tables is a dict of dataframes
    if isinstance(tables, dict):
        # rename tables titles ("Current Storage IO" --> "Storage IO", "Current Netowrk Usage" --> "Network Usage")
        tables_dict = tables
        tables_dict['Storage IO'] = tables_dict.pop('Current Storage IO')
        tables_dict['Network Usage'] = tables_dict.pop('Current Network Usage')
        # rename metrics in dataframes
        for title, table_df in tables_dict.items():
            tables_dict[title] = rename_df_metrics(table_df)
        return tables_dict

    # tables is a list of dataframes
    if isinstance(tables, dict):
        tables_list = []
        for table_df in tables:
            tables_list.append(rename_df_metrics(table_df))
        return tables_list

    # If tables is not one of the above, there is a wrong user input
    raise ValueError("tables must either be a dataframe or a dictionary of dataframes, a list of dataframes.")



'''
========================================
            User functions
========================================
'''

# given a dataframe of runs, and list of indices of runs in the dataframe, (can also specify as_one_df and only_include_worker_pods)
# return a dataframe (if as_one_df==True) of all of the tables queried for those runs
# or return a list of dataframes (if_as_one_df==False) of all the tables queried for that run
def get_tables_for_many_runs(runs_df, run_indices, as_one_df=False, only_include_worker_pods=False):
    # get tables class to be able to use methods for querying tables
    tables_class = Tables(namespace=NAMESPACE)

    # get run start times as datetimes
    runs_df['start'] = runs_df['start'].apply(datetime_ify)

    # get all the selected runs into a single df to iterate over
    selected_runs_df = runs_df.iloc[run_indices]

    # get tables as a single dataframe for each run, add it to dfs_list
    dfs_list = []
    for index, run in selected_runs_df.iterrows():
        # get duration and start of run
        start = run['start']
        duration_seconds = run['runtime']

        # get queries and partial_queries to be passed into tables_class methods
        run_queries, run_partial_queries = get_queries(all_query_bodies_dict, all_metrics_dict, start, duration_seconds)

        # get tables as one df from queries
        tables_df = tables_class.get_tables_as_one_df(
            only_include_worker_pods=only_include_worker_pods, 
            queries=run_queries, 
            partial_queries=run_partial_queries)
        # fill in missing values in requests and limits
        tables_df = fill_in_static_na(tables_df, "cpu")
        tables_df = fill_in_static_na(tables_df, "mem")
        
        # add tables_df to table_runs_df
        tables_df = rename_tables(tables_df)
        tables_df.insert(0,'run_id', run['run_uuid'])
        tables_df.insert(0,'run_index', index)
        dfs_list.append(tables_df)

    # get all runs as a single dataframe
    if as_one_df:
        table_runs_df = pd.concat(dfs_list, ignore_index=True)
        return table_runs_df
    
    # otherwise, return a list of dataframes
    return dfs_list


# given a dataframe of runs, and an index of a run in the dataframe, (can also specify as_one_df and only_include_worker_pods)
# return a dataframe (if as_one_df==True) of all of the tables queried for that run
# or return a dict of dataframes (if_as_one_df==False) of all the tables queried for that run separated by table type
def get_tables_for_one_run(runs_df, run_index, as_one_df=False, only_include_worker_pods=False):
    # get tables class to be able to use methods for querying tables
    tables_class = Tables(namespace=NAMESPACE)

    # get run start times as datetimes and select run to use
    runs_df['start'] = runs_df['start'].apply(datetime_ify)
    run = runs_df.iloc[run_index]

    # get duration and start of run
    start = run['start']
    duration_seconds = run['runtime']

    # get queries and partial_queries to be passed into tables_class methods
    run_queries, run_partial_queries = get_queries(all_query_bodies_dict, all_metrics_dict, start, duration_seconds)

    # get tables as one df from queries
    if as_one_df:
        tables_df = tables_class.get_tables_as_one_df(
            only_include_worker_pods=only_include_worker_pods, 
            queries=run_queries, 
            partial_queries=run_partial_queries)
        # fill in missing values in requests and limits
        tables_df = fill_in_static_na(tables_df, "cpu")
        tables_df = fill_in_static_na(tables_df, "mem")
        # rename tables
        tables_df = rename_tables(tables_df)
        return tables_df

    # otherwise get tables as dict of dfs
    tables_dict = tables_class.get_tables_dict(
    only_include_worker_pods=only_include_worker_pods, 
    queries=run_queries, 
    partial_queries=run_partial_queries)
    # fill in missing values in requests and limits
    tables_dict['CPU Quota'] = fill_in_static_na(tables_dict['CPU Quota'], "cpu")
    tables_dict['Memory Quota'] = fill_in_static_na(tables_dict['Memory Quota'], "mem")
    # rename tables
    tables_dict = rename_tables(tables_dict)

    return tables_dict

'''
=========================================================
metrics and corresponding query bodies sorted by category
=========================================================
'''
static_query_bodies = {
    'CPU Requests': 'cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests',
    'CPU Limits': 'cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits',
    'Memory Requests': 'cluster:namespace:pod_memory:active:kube_pod_container_resource_requests',
    'Memory Limits': 'cluster:namespace:pod_memory:active:kube_pod_container_resource_limits'
}

increase_query_bodies = {
    'CPU Usage': 'container_cpu_usage_seconds_total',
    'Current Receive Bandwidth': 'container_network_receive_bytes_total',
    'Current Transmit Bandwidth': 'container_network_transmit_bytes_total',
    'Rate of Received Packets': 'container_network_receive_packets_total',
    'Rate of Transmitted Packets': 'container_network_transmit_packets_total',
    'Rate of Received Packets Dropped': 'container_network_receive_packets_dropped_total',
    'Rate of Transmitted Packets Dropped': 'container_network_transmit_packets_dropped_total',
    'IOPS(Reads)': 'container_fs_reads_total',
    'IOPS(Writes)': 'container_fs_writes_total',
    'Throughput(Read)': 'container_fs_reads_bytes_total',
    'Throughput(Write)': 'container_fs_writes_bytes_total'
}

max_query_bodies = {
    'Memory Usage': 'container_memory_working_set_bytes',
    'Memory Usage (RSS)': 'container_memory_rss',
    'Memory Usage (Cache)': 'container_memory_cache'
}


all_query_bodies_dict = {
    'static': static_query_bodies,
    'increase': increase_query_bodies,
    'max': max_query_bodies
}


'''
========================================================
metrics sorted by tables (for feeding into tables_class)
========================================================
'''
metrics = {
    # CPU Quota
    'CPU Usage',
    'CPU Requests',
    'CPU Limits',
    # Memory Quota
    'Memory Usage',
    'Memory Requests',
    'Memory Limits',
    'Memory Usage (RSS)',
    'Memory Usage (Cache)',
    # Network Usage
    'Current Receive Bandwidth',
    'Current Transmit Bandwidth',
    'Rate of Received Packets',
    'Rate of Transmitted Packets',
    'Rate of Received Packets Dropped',
    'Rate of Transmitted Packets Dropped'
}
partial_metrics = {
    # Input Output
    'IOPS(Reads)',
    'IOPS(Writes)',
    'Throughput(Read)',
    'Throughput(Write)'
}


all_metrics_dict = {
    'metrics':metrics,
    'partial':partial_metrics
}


'''
============================
        Main Program        
============================
'''

# get a dataframe of runs
runs_df = pd.read_csv(read_file, index_col=0)

# get tables data for one run
run_index = 50 # can pick any run between 0 and len(df)-1 inclusive
run_tables = get_tables_for_one_run(
    runs_df=runs_df,
    run_index=run_index,
    as_one_df=True, # if set to False, returns a dictionary of titles, tables
    only_include_worker_pods=False # if set to True, only includes bp3d-worker pods and changes their name to be just their ensemble id
    )
print(run_tables)


'''
# get tables data for multiple runs
run_indices = [50, 60, 70] # can pick any runs between 0 and len(df)-1 inclusive
runs_tables_df = get_tables_for_many_runs(
    runs_df=runs_df,
    run_indices=run_indices,
    as_one_df=True, # if set to False, returns a dictionary of titles, tables
    only_include_worker_pods=False # if set to True, only includes bp3d-worker pods and changes their name to be just their ensemble id
    )
print(runs_tables_df)
'''
