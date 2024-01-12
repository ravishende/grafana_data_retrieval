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
================
Helper functions
================
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
    # get offset for query
    # offset = calculate_offset(start, duration_seconds)
    # query static metrics 5 seconds after run started instead of as run ends - fewer NA pods for limits and requests
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


# Returns a dataframe with changed names of columns that contain "IOPS" to use "IO" instead.
# Reason: in tables.py, it is usually giving a snapshot present, so IO per second
# (IOPS) is useful. But in this case, we are looking over an entire run, so we want
# the total IO of the run rather than the average IOPS of the run.
# Note: The updated queries already do this, we just have to change the names to match,
# after we do the querying from the tables_class methods.
def update_df_IOPS_naming(df):
    # for every column name that contains "IOPS", replace "IOPS" with "IO"
    col_rename_dict = {col: col.replace("IOPS", "IO") for col in df.columns}
    renamed_df = df.rename(columns=col_rename_dict)
    return renamed_df

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




'''
============================
        Main Program        
============================
'''

# select a run from the dataframe of runs
df = pd.read_csv(read_file, index_col=0)
df['start'] = df['start'].apply(datetime_ify) # get run start times as datetimes
run_index = 90 # can pick any run between [0,len(df)) so between 0 and 191
run = df.iloc[run_index] 

# get duration and start of run
start = run['start']
duration_seconds = run['runtime']

# assemble queries
max_queries = assemble_max_queries(max_query_bodies, start, duration_seconds)
increase_queries = assemble_increase_queries(increase_query_bodies, start, duration_seconds)
static_queries = assemble_static_queries(static_query_bodies, start, duration_seconds)

# get all queries into one dictionary
unsorted_queries = {}
unsorted_queries.update(max_queries)
unsorted_queries.update(increase_queries)
unsorted_queries.update(static_queries)

# assemble queries and partial_queries for the run - sorted versions of the unsorted_queries separated by full queries vs partial_queries
run_queries = {key: unsorted_queries[key] for key in metrics}
run_partial_queries = {key: unsorted_queries[key] for key in partial_metrics}

tables_class = Tables(namespace=NAMESPACE)

'''
# for getting as a single larger dataframe instead of a dict of dataframes
# get tables as one df from queries
tables_df = tables_class.get_tables_as_one_df(
    only_include_worker_pods=False, 
    queries=run_queries, 
    partial_queries=run_partial_queries)
# fill in missing values in requests and limits
tables_df = fill_in_static_na(tables_df, "cpu")
tables_df = fill_in_static_na(tables_df, "mem")
# print data
tables_df = update_df_IOPS_naming(tables_df)
print(tables_df)
# output df to a csv file
# write_file = "output.csv"
# tables_df.to_csv(write_file)
'''

# '''
# for getting as a dict of table dataframes instead of one large df:
# get tables as dict of dfs from queries
tables_dict = tables_class.get_tables_dict(
    only_include_worker_pods=False, 
    queries=run_queries, 
    partial_queries=run_partial_queries)
# fill in missing values in requests and limits
tables_dict['CPU Quota'] = fill_in_static_na(tables_dict['CPU Quota'], "cpu")
tables_dict['Memory Quota'] = fill_in_static_na(tables_dict['Memory Quota'], "mem")
# rename "Current Storage IO" to be "Storage IO" and "Current Netowrk Usage" to be "Network Usage"
tables_dict['Storage IO'] = tables_dict.pop('Current Storage IO')
tables_dict['Network Usage'] = tables_dict.pop('Current Network Usage')
# update IOPS column names in Storage IO df to be IO instead of IOPS
tables_dict['Storage IO'] = update_df_IOPS_naming(tables_dict['Storage IO'])
# print data
print_dataframe_dict(tables_dict)
# '''
