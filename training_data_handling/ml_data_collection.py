import pandas as pd
from pprint import pprint
from datetime import datetime, timedelta
from random import randint
import csv
# get set up to be able to import files from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("ml_data_collection.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.querying import query_api_site, get_result_list, query_api_site_for_graph
from helpers.printing import print_title
from tables import Tables

'''
---------------------------------------------------------------------------
---------------------------------------------------------------------------
---------------------------------------------------------------------------
NOTE:
This file is outdated. Instead, use ensemble_total_resource_consumption.py
---------------------------------------------------------------------------
---------------------------------------------------------------------------
---------------------------------------------------------------------------
'''


tables_class = Tables()

# takes in a time in seconds since epoch (float or int), pandas Timestamp(), or datetime object formats
# returns the time as a datetime object
def convert_to_datetime(time):
    # check if time is a pandas Timestamp()
    # technically this counts as an instance of type datetime but is not the same
    # so we must check if time is a pandas Timestamp() before checking if it's a datetime object
    if isinstance(time, pd.Timestamp):
        return time.to_pydatetime(warn=False)
    
    # check if time is a float (seconds since the epoch: 01/01/1970)
    if isinstance(time, float) or isinstance(time, int):
        return datetime.fromtimestamp(time)
    
    # check if time is a datetime object
    if isinstance(time, datetime):
        return time

# assembles string for the time filter to be passed into query_api_site_for_graph()
def assemble_time_filter(start, end, time_step=None):
    if time_step is None:
        # get time_step to be larger than duration so that there is only one datapoint per pod
        duration_seconds = (end - start).total_seconds()
        time_step = duration_seconds + 1;
    # make sure both start and end are datetime objects
    start = convert_to_datetime(start)
    end = convert_to_datetime(end)

    # assemble strings
    end_str = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # combine strings into time filter format
    time_filter = f'start={start_str}&end={end_str}&step={time_step}'

    return time_filter

def update_pods_nodes_count(result_list, pods_count, nodes_count):
    for result in result_list:
        node = result['metric']['node']
        pod = result['metric']['pod']
        if pod in pods_count:
            pods_count[pod] += 1
            nodes_count[node] += 1
        else:
            pods_count[pod] = 1;
            if node in nodes_count:
                nodes_count[node] += 1;
            else:
                nodes_count[node] = 1;
    return pods_count, nodes_count



print("Querying Data:")

# query = tables_class.queries[0]
# node_namespace_pod_container:
query = 'sum by(node, pod) (increase(node_namespace_pod_container:container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[1h]))',
pods_count = {}
nodes_count = {}

# finding the performance data for each training datapoint
# read in csv
with open('old_training_data.csv', mode='r') as training_data:
    data = csv.DictReader(training_data)
    line_count = 0
    for row in data:
        # collect start time and runtime
        runtime_seconds = int(row['runtime'])
        start = datetime.now() - timedelta(seconds=runtime_seconds)

        # generate a random time for querying performance data
        collection_seconds = randint(1, runtime_seconds)
        collection_time = start + timedelta(seconds=collection_seconds)

        # query for the data from the start time til the random refresh time 
        time_filter = assemble_time_filter(start=start, end=collection_time)
        result_list = get_result_list(query_api_site_for_graph(tables_class.queries['CPU Usage'], time_filter))
        
        # update which pods and nodes show up and how frequently
        pods_count, nodes_count = update_pods_nodes_count(result_list, pods_count, nodes_count)
        print_title("Datapoint index: " + str(line_count) + " || end (seconds since start): " + str(collection_seconds))  
        # pprint(result_list)

        # collect 1000 rows worth of data
        line_count+=1
        if(line_count >= 1000):
            break
    print_title("Nodes Count:")
    pprint(nodes_count)

    print_title("Pods Count:")
    pprint(pods_count)
# # get csv as pandas dataframe
# # training_df = pd.read_csv("old_training_data.csv")


# # finding the performance data for each training datapoint
# for row in training_df:
#     if(row == 0):
#         for col_title in row:
#             print(col_title, "|| ", end="")
#     # collect start time and runtime
#     runtime_seconds = int(row['runtime'])
#     start = datetime.now() - timedelta(seconds=runtime_seconds)

#     # generate a random time for querying performance data
#     collection_seconds = randint(1, runtime_seconds)
#     collection_time = start + timedelta(seconds=collection_seconds)

#     time_filter = assemble_time_filter(start=start, end=collection_time)

#     queried_data = query_api_site_for_graph(tables_class.queries['CPU Usage'], time_filter)
#     print_title(line_count)
#     pprint(queried_data)
#     line_count+=1

# # finding the performance data for each training datapoint
# for datapoint in training_data:
#     # collect start time and runtime
#     # start = datapoint['start'].to_datetime()
#     runtime_seconds = datapoint['runtime']
#     start = datetime.now() - runtime_seconds

#     # generate a random time for querying performance data
#     collection_seconds = randint(1, runtime_seconds)
#     collection_time = start + timedelta(seconds=collection_seconds)

#     time_filter = assemble_time_filter(start=start, end=collection_time)

#     query_api_site_for_graph(tables_class.queries['CPU Usage'], time_filter)


# tables_class = Tables()
# tables_dict = tables_class.get_tables_dict()

# query = 'sum by(node, pod, cluster) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"})'

# portions = [
#     'sum by(node, pod) (',
#     'node_namespace_pod_container:',
#     'container_cpu_usage_seconds_total:',
#     'sum_irate{cluster="", namespace="wifire-quicfire"}'
# ]

# query = 'container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[5m]'
# # query = 'node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="alto"}'
# queried_data = query_api_site(query)
# pprint(queried_data)
# print ("||", len(get_result_list(queried_data)), "||")

# print("\n"*5, "*"*100, "\n"*5)
# end = datetime.now()
# end_str = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
# start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
# combine strings into time filter format
# time_filter = f'start={start_str}&end={end_str}&step={time_step}'

# query2 = 'node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="wifire-quicfire"}'
# query2 = 'sum by(node, pod) (container_cpu_usage_seconds_total{namespace="wifire-quicfire"})'
# query2 = 'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="alto"})'
# queried_data2 = query_api_site(query2)
# pprint(queried_data2)
# print ("||", len(get_result_list(queried_data2)), "||")
