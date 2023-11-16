import pandas as pd
from datetime import datetime, timedelta
from termcolor import colored
# get set up to be able to import files from parent directory (grafana_data_retrieval)
# utils.py and inputs.py not in this current directory and instead in the parent
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("ensemble_total_resource_metrics.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from utils import query_api_site, query_api_site_for_graph, get_result_list, print_title

# Settings - You can edit these, especially NUM_ROWS, which is how many rows to generate per run
pd.set_option('display.max_columns', None)
csv_file = 'performance_training_data.csv'
NUM_ROWS = 500
NAMESPACE = 'wifire-quicfire'

# For printing rows. Do not edit these.
CURRENT_CPU_ROW = 1
CURRENT_MEM_ROW = 1

'''
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
NOTE:
This file reads in a csv called performance_training_data.csv, 
which contains all the training data from Devin's inputs as well as columns
for cpu_usage_total and mem_usage total. Running this appends NUM_ROWS values 
for those two columns to what has already been queried for, filling out more and 
more of the datapoints. 

All that needs to be done is select NUM_ROWS to be the value you would like and then run the file.
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------

'''

# get the csv file as a pandas dataframe
training_data = pd.read_csv(csv_file, index_col=0)

'''
---------------------------------------
            Helper Functions
---------------------------------------
'''

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


# given a start and stop time (also row_index and n_rows for printing purposes),
# query for total cpu usage of the entire run and print updates on rows completed
def get_cpu_total(start, stop, row_index, n_rows):
    # get start and stop as datetime objects
    start = datetime_ify(start)
    stop = datetime_ify(stop)
    
    # assemble query
    duration, offset = calculate_duration_and_offset(start, stop)
    total_cpu_query = 'sum by (node, pod) (increase(container_cpu_usage_seconds_total{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    # gather data
    total_cpu_data = get_result_list(query_api_site(total_cpu_query))
    
    # print row information
    global CURRENT_CPU_ROW
    progress_message = "Row complete: " +  str(CURRENT_CPU_ROW) + " / " +  str(n_rows)
    print(colored(progress_message, "green"))
    print("row index:", row_index)
    CURRENT_CPU_ROW += 1

    return total_cpu_data


# given a start and stop time (also row_index and n_rows for printing purposes),
# query for total memory usage of the entire run and print updates on rows completed
def get_mem_total(start, stop, row_index, n_rows):
    # get start and stop as datetime objects
    start = datetime_ify(start)
    stop = datetime_ify(stop)
    
    # assemble query
    duration, offset = calculate_duration_and_offset(start, stop)
    total_mem_query = 'sum by (node, pod) (increase(container_memory_working_set_bytes{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    # gather data
    total_mem_data = get_result_list(query_api_site(total_mem_query))
    
    # print row information
    global CURRENT_MEM_ROW
    progress_message = "Row complete: " +  str(CURRENT_MEM_ROW) + " / " +  str(n_rows)
    print(colored(progress_message, "green"))
    print("row index:", row_index)
    CURRENT_MEM_ROW += 1

    return total_mem_data


'''
---------------------------------------
            User Function
---------------------------------------
'''

# given a dataframe and number of rows, calculate performance data columns for those rows
# starting from the first None value. The other rows' values for those columns will be unchanged.
# returns the updated dataframe.
def insert_performace_cols(df, n_rows):
    # get first row without CPU usage data, find last row to generate to
    start_row = df['cpu_usage'].isna().idxmax()
    end_row = start_row + n_rows - 1
    
    # make sure you don't try to query past the end of the dataframe
    last_index = len(df) - 1
    if(end_row > last_index):
        end_row = last_index
        n_rows = end_row - start_row + 1
        print(colored("\n\nEnd row is greater than last index. Only generating to last index.", "yellow"))

    # if no na values, there is nothing to generate.
    if start_row == 0 and df['cpu_usage'][0]:
        print(colored("\n\nNo NA rows", "yellow"))
        return df
    
    # add values for cpu_usage and mem_usage columns starting from start_row and doing it for n_rows rows.
    print_title("Inserting CPU Data")
    df['cpu_usage'] = df.apply(
        lambda row: get_cpu_total(row['start'], row['stop'], row.name, n_rows) if start_row <= row.name <= end_row else row['cpu_usage'],
        axis=1)  # Note: row.name is just the index of the row
    
    print_title("Inserting Memory Data")
    df['mem_usage'] = df.apply(
        lambda row: get_mem_total(row['start'], row['stop'], row.name, n_rows) if start_row <= row.name <= end_row else row['mem_usage'],
        axis=1)
    return df


# calculate performance data
training_data = insert_performace_cols(training_data, NUM_ROWS)
print("\n"*5, training_data)

# write updated df to a csv file
training_data.to_csv(csv_file)

# Todo: 
# get cpu_usage and mem_usage columns to a single number
    # filter cpu_usage and mem_usage columns to only include pods in the correct ensemble. 
    # then sum over all pods to get total cpu_usage for the run.
# add more columns:
    # one for cpu total usage at some randomized refresh time
    # one for memory total usage at some randomized refresh time
    # one for refresh time measured in seconds since the start time
    # one for IO information
    # one for network info
