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
    offset = calculate_offset(start, duration_seconds)

    # get prefix of query ready for assembly (suffix gets defined while looping over query_bodies)
    prefix = "sum by (node, pod) ("

    # get the right suffix depending on the resource
    suffixes = {
         # set resource to cpu and multiply by duration seconds to get metric from measuring cpu cores to cpu seconds
        'cpu':'{resource="cpu", namespace="' + NAMESPACE + '"} offset ' + str(offset) + ') * ' + str(duration_seconds),
        # set resource to memory
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
run_index = 50 # can pick any run between [0,len(df))
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

# Print information on the run and queries
print('\n'*5, "run:", run, sep='\n')
print('\n'*5, "Queries:", run_queries, "\n"*5, sep='\n')

# get tables from queriess
tables_class = Tables(namespace=NAMESPACE)
tables_dict = tables_class.get_tables_dict(
    only_include_worker_pods=False, 
    queries=run_queries, 
    partial_queries=run_partial_queries)


print_dataframe_dict(tables_dict)