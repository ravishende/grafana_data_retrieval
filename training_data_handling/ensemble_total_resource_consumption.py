import pandas as pd
from datetime import datetime, timedelta
# get set up to be able to import files from parent directory (grafana_data_retrieval)
# utils.py and inputs.py not in this current directory and instead in the parent
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("ensemble_total_resource_metrics.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from utils import query_api_site, query_api_site_for_graph, get_result_list, print_title

# settings and constants
pd.set_option('display.max_columns', None)
NUM_ROWS = 10
NAMESPACE = 'wifire-quicfire'
CURRENT_ROW = 1

# collect NUM_ROWS rows from the training data csv as a pandas DataFrame
training_data = pd.read_csv('cleaned_training_data.csv', nrows=NUM_ROWS)

# given a timedelta, get it in the form 2d4h12m30s for use with querying
def delta_to_time_str(delta):
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_str = f"{days}d{hours}h{minutes}m{seconds}s"
    return time_str


# given a start and stop time, figure out how long ago the stop time was and how long of a duration the run was for
def calculate_duration_and_offset(start, end):
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        raise ValueError("start and end must be datetime objects")

    duration = delta_to_time_str((end-start))

    offset_delta = datetime.now() - end
    offset = delta_to_time_str(offset_delta) #convert offset from timedelta to time string

    return duration, offset


# convert a time string into a datetime object
def datetime_ify(time):
    # get time as datetime object. Time format should be one of two patterns.
    try:
        # get time down to the second, no decimal seconds.
        time = time[0:time.find(".")]
        # get time as datetime
        format_string = "%Y-%m-%d %H:%M:%S"
        time = datetime.strptime(time, format_string)
        return time
    except ValueError:
        # get start and stop as datetimes, then find the difference between them for the runtime
        format_string = "%Y-%m-%dT%H:%M:%S"
        time = datetime.strptime(time, format_string)
        return time


# given a start and stop time, query for total cpu usage of the entire run
def get_cpu_total(start, stop):
    # get start and stop as datetime objects
    start = datetime_ify(start)
    stop = datetime_ify(stop)
    # assemble query
    duration, offset = calculate_duration_and_offset(start, stop)
    total_cpu_query = 'sum by (node, pod) (increase(container_cpu_usage_seconds_total{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    # gather data
    total_cpu_data = get_result_list(query_api_site(total_cpu_query))
    return total_cpu_data

# given a start and stop time, query for total cpu usage of the entire run
def get_mem_total(start, stop):
    # get start and stop as datetime objects
    start = datetime_ify(start)
    stop = datetime_ify(stop)
    # assemble query
    duration, offset = calculate_duration_and_offset(start, stop)
    total_mem_query = 'sum by (node, pod) (increase(container_memory_working_set_bytes{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    # gather data
    total_mem_data = get_result_list(query_api_site(total_mem_query))
    # print row information
    global CURRENT_ROW
    print("Row complete:", CURRENT_ROW, "/", NUM_ROWS)
    CURRENT_ROW += 1
    return total_mem_data

# for each row in the dataframe, query for the total cpu and memory usage of the run
training_data['cpu_usage'] = training_data.apply(lambda row: get_cpu_total(row['start'], row['stop']), axis=1)
training_data['mem_usage'] = training_data.apply(lambda row: get_mem_total(row['start'], row['stop']), axis=1)

print("\n"*5)
print(training_data)

training_data.to_csv("queried_training_data.csv")

# pprint(total_mem_data)
# print(len(total_mem_data))
