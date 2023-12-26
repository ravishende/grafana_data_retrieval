import pandas as pd
from datetime import datetime, timedelta
from termcolor import colored
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("query_resources.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.querying import query_data
from helpers.printing import print_title


# Settings - You can edit these, especially NUM_ROWS, which is how many rows to generate per run
pd.set_option('display.max_columns', None)
csv_file = 'csv_files/queried.csv'
NUM_ROWS = 1000
NAMESPACE = 'wifire-quicfire'

# For printing rows. Do not edit.
CURRENT_ROW = 1

'''
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
NOTE:
This file reads in a csv which contains all the training data from Devin's inputs as well as columns
for cpu_total and mem_total usage for the run. Running this appends NUM_ROWS values 
for those two columns to what has already been queried for, filling out more and 
more of the datapoints. This can be edited in the main program portion of the file to query for any cpu and memory
usage up until a certain duration. The only requirement is adding that new duration column.

All that needs to be done is select NUM_ROWS to be the value you would like and then run the file.
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------

'''


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


# convert a time string into a datetime object
def datetime_ify(time):
    # handle if it is already of type pandas datetime or actual datetime
    if isinstance(time, pd.Timestamp):
        return time.to_pydatetime(warn=False)
    if isinstance(time, datetime):
        return time

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


# given a start (datetime object) of a run and duration in seconds, 
# return the offset of the end of the run from the current time
def calculate_offset(start, duration):
    # check for proper inputs
    if not isinstance(start, datetime):
        raise ValueError("start must be a datetime object")
    try:
        duration = float(duration)
    except ValueError:
        raise ValueError("duration must be a float or int")

    # calculate offset from current time by finding end time and subtracting now from end
    end = start + timedelta(seconds=duration)
    offset_delta = datetime.now() - end
    offset = delta_to_time_str(offset_delta) #convert offset from timedelta to time string

    return offset

# given a resource ("cpu" or "mem"), start (datetime), and duration (float)
# return a query for the resource over the given duration of the run
def get_resource_query(resource, start, duration):
    # all resources and the heart of their queries
    query_bodies = {
        "cpu":"container_cpu_usage_seconds_total",
        "mem":"container_memory_working_set_bytes"
    }

    # check if user input resource is in known resources
    resources = query_bodies.keys()
    if resource not in resources:
        raise ValueError(f'resouce "{resource}" must be within one of the following resources:\n{resources}')

    # get all the pieces necessary to assemble the query
    offset = calculate_offset(start, duration)
    duration = delta_to_time_str(timedelta(seconds=duration))
    prefix = 'sum by (node, pod) (increase('
    suffix = '{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'

    # assemble the final query
    query = prefix + query_bodies[resource] + suffix
    return query

# Given a resource ("mem" or "cpu"), start of a run (time string), and duration (float or int)
# (Also takes in row_index and n_rows for printing purposes)
# return the queried data of that resource over the duration of the run
def query_resource(resource, start, duration, row_index, n_rows):
    # query for data
    start = datetime_ify(start)
    query = get_resource_query(resource, start, duration)
    resource_data = query_data(query)

    # print row information
    global CURRENT_ROW
    progress_message = f"Row complete: {CURRENT_ROW} / {n_rows}"
    print(colored(progress_message, "green"))
    print("Row index:", row_index)
    CURRENT_ROW += 1

    # return queried data
    return resource_data


'''
---------------------------------------
            User Function
---------------------------------------
'''

# Given a dataframe, resource ("cpu" or "mem") name of the column to insert,
#  name of the duration column to use for calculating, and the number of rows to query for:
# calculate performance data columns for those rows starting from the first None value. 
# The other rows' values for those columns will be unchanged.
# Returns the updated dataframe.
def insert_column(df, resource, insert_col, duration_col, n_rows):
    # handle if column doesn't exist
    if insert_col not in df.columns:
        df[insert_col] = None

    # Calculate start and end rows based on n_rows
    start_row = df[insert_col].isna().idxmax()
    end_row = start_row + n_rows - 1
    
    # make sure you don't try to query past the end of the dataframe
    last_index = len(df) - 1
    if(end_row > last_index):
        end_row = last_index
        n_rows = end_row - start_row + 1
        print(colored("\n\nEnd row is greater than last index. Only generating to last index.", "yellow"))

    # if no na values, there is nothing to generate.
    if start_row == 0 and df[insert_col][0]:
        print(colored("\n\nNo NA rows", "yellow"))
        return df
    
    # since we're starting a new column, reset the current printing row to 1
    # this CURRENT_ROW is used for printing in the query_resource function
    global CURRENT_ROW
    CURRENT_ROW = 1

    # Query metric and insert column into dataframe
    print_title(f"Inserting {insert_col}")
    df[insert_col] = df.apply(
        lambda row: query_resource(resource, row['start'], row[duration_col], row.name, n_rows) \
        if start_row <= row.name <= end_row else row[insert_col], axis=1)  
        # Note: row.name is just the index of the row
    
    return df



'''
---------------------------------------
            Main Program
---------------------------------------
'''

# get the csv file as a pandas dataframe
training_data = pd.read_csv(csv_file, index_col=0)
n_rows = NUM_ROWS

# calculate total performance data
training_data = insert_column(training_data, "cpu", "cpu_total", 'runtime', n_rows)
training_data = insert_column(training_data, "mem", "mem_total", 'runtime', n_rows)

'''
# calculate performance data up to a point
training_data = insert_column(training_data, "cpu", "cpu_t1", 'duration_t1', n_rows)
training_data = insert_column(training_data, "mem", "mem_t2", 'duration_t1', n_rows)
training_data = insert_column(training_data, "cpu", "cpu_t2", 'duration_t2', n_rows)
training_data = insert_column(training_data, "mem", "mem_t2", 'duration_t2', n_rows)
'''
# print and write updated df to a csv file
print("\n"*5, training_data)
training_data.to_csv(csv_file)

# Todo: 
# add more columns:
    # one for some random refresh time measured in seconds since the start time
    # one for cpu total usage at refresh time
    # one for memory total usage at refresh time
    # one for IO information
    # one for network info





'''
---------------------------------------
            Old Functions
---------------------------------------


# given a start and stop time, figure out how long ago the stop time was and how long of a duration the run was for
def calculate_duration_and_offset(start, end):
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        raise ValueError("start and end must be datetime objects")

    duration = delta_to_time_str((end-start))

    offset_delta = datetime.now() - end
    offset = delta_to_time_str(offset_delta) #convert offset from timedelta to time string

    return duration, offset


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
    total_mem_data = query_api_site(total_mem_query)
    
    # print row information
    global CURRENT_MEM_ROW
    progress_message = "Row complete: " +  str(CURRENT_MEM_ROW) + " / " +  str(n_rows)
    print(colored(progress_message, "green"))
    print("row index:", row_index)
    CURRENT_MEM_ROW += 1

    return total_mem_data

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




'''
