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
# pd.set_option('display.max_colwidth', 30)



# return a query for the resource over the given duration of the run
def assemble_queries(query_bodies, start, duration, static=False, resource=""):
    # get all the pieces necessary to assemble the query
    offset = calculate_offset(start, duration)
    duration_int = duration
    duration = delta_to_time_str(timedelta(seconds=duration))
    prefix = 'sum by (node, pod) (increase('
    suffix = '{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    
    # some metrics are static throughout an entire run (limits, requests). These queries are different
    static_prefix = 'sum by (node, pod) ('
    static_suffixes = {
        'cpu':'{resource="cpu", namespace="' + NAMESPACE + '"} offset ' + str(offset) + ') * ' + str(duration_int),
        'mem':'{resource="memory", namespace="' + NAMESPACE + '"} offset ' + str(offset) + ')'
    }
    
    # assemble the final queries
    queries = query_bodies
    for title, query_body in query_bodies.items():
        query = ""
        if static:
            query = static_prefix + query_body + static_suffixes[resource]
        else:
            query = prefix + query_body + suffix
        queries[title] = query

    return queries





query_bodies = {
    # CPU Quota
    'CPU Usage': 'container_cpu_usage_seconds_total',
      # static metrics:
        # 'CPU Requests': 'cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests',
        # 'CPU Limits': 'cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits',
    # Memory Quota
    'Memory Usage': 'container_memory_working_set_bytes',
      # static metrics:
        # 'Memory Requests': 'cluster:namespace:pod_memory:active:kube_pod_container_resource_requests',
        # 'Memory Limits': 'cluster:namespace:pod_memory:active:kube_pod_container_resource_limits',
    'Memory Usage (RSS)': 'container_memory_rss',
    'Memory Usage (Cache)': 'container_memory_cache',
    # Network Usage
    'Current Receive Bandwidth': 'container_network_receive_bytes_total',
    'Current Transmit Bandwidth': 'container_network_transmit_bytes_total',
    'Rate of Received Packets': 'container_network_receive_packets_total',
    'Rate of Transmitted Packets': 'container_network_transmit_packets_total',
    'Rate of Received Packets Dropped': 'container_network_receive_packets_dropped_total',
    'Rate of Transmitted Packets Dropped':  'container_network_transmit_packets_dropped_total'
}
partial_query_bodies = {
    'IOPS(Reads)': 'container_fs_reads_total',
    'IOPS(Writes)': 'container_fs_writes_total',
    'Throughput(Read)': 'container_fs_reads_bytes_total',
    'Throughput(Write)': 'container_fs_writes_bytes_total'
}

static_cpu_query_bodies = {
    'CPU Requests': 'kube_pod_container_resource_requests',
    'CPU Limits': 'kube_pod_container_resource_limits'
}
static_mem_query_bodies = {
    'Memory Requests': 'kube_pod_container_resource_requests',
    'Memory Limits': 'kube_pod_container_resource_limits'
}

# select a run from the dataframe of runs
df = pd.read_csv(read_file, index_col=0, nrows=21)
df['start'] = df['start'].apply(datetime_ify)
run = df.iloc[20]

# get duration and start
start = run['start']
duration = run['runtime']

# assemble queries and print information
run_queries = assemble_queries(query_bodies, start, duration)
static_cpu_queries = assemble_queries(static_cpu_query_bodies, start, duration, static=True, resource='cpu')
static_mem_queries = assemble_queries(static_mem_query_bodies, start, duration, static=True, resource='mem')
run_queries.update(static_cpu_queries)
run_queries.update(static_mem_queries)
run_partial_queries = assemble_queries(partial_query_bodies, start, duration)


def multiply_cols_by_duration(df, duration, cols_to_update):
    for col_title in cols_to_update:
        df[col_title] = pd.to_numeric(df[col_title]) * duration
    return df


tables_class = Tables(namespace=NAMESPACE)
tables_dict = tables_class.get_tables_dict(
    only_include_worker_pods=False, 
    queries=run_queries, 
    partial_queries=run_partial_queries)

print_dataframe_dict(tables_dict)