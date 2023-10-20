from tables import Tables
import pandas as pd
from utils import query_api_site, get_result_list, query_api_site_for_graph, print_title
from pprint import pprint
from datetime import datetime, timedelta
from random import randint
import csv

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


print("Querying Data:")

# finding the performance data for each training datapoint
# read in csv
with open('training_data.csv', mode='r') as training_data:
    data = csv.DictReader(training_data)
    line_count = 0
    for row in data:
        # collect start time and runtime
        runtime_seconds = int(row['runtime'])
        start = datetime.now() - timedelta(seconds=runtime_seconds)

        # generate a random time for querying performance data
        collection_seconds = randint(1, runtime_seconds)
        collection_time = start + timedelta(seconds=collection_seconds)

        time_filter = assemble_time_filter(start=start, end=collection_time)

        queried_data = query_api_site_for_graph(tables_class.queries['CPU Usage'], time_filter)
        print_title("Datapoint index: " + str(line_count) + " || end (seconds since start): " + str(collection_seconds))
        pprint(queried_data)
        line_count+=1

# # get csv as pandas dataframe
# # training_df = pd.read_csv("training_data.csv")


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
