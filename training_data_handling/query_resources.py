import pandas as pd
from datetime import datetime, timedelta
from termcolor import colored
import random
import shutil
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("query_resources.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.querying import query_data
from helpers.printing import print_title
from helpers.time_functions import delta_to_time_str, datetime_ify, calculate_offset


# Settings - You can edit these, especially NUM_ROWS, which is how many rows to generate per run
csv_file = 'csv_files/queried_w_ids.csv'
NUM_ROWS = 1000
NAMESPACE = 'wifire-quicfire'

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# For printing rows. Do not edit.
CURRENT_ROW = 1

'''
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
This file reads in a csv which contains all the training data from Devin's inputs as well as columns
for cpu_total, cpu_t1, cpu_t2, mem_total, mem_t1, and mem_t2 usage for the run. Running this appends NUM_ROWS values 
for those 6 columns to what has already been queried for, filling out more and 
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

# given a resource ("cpu" or "mem"), start (datetime), and duration (float)
# return a query for the resource over the given duration of the run
def get_resource_query(resource, start, duration):
    # all resources and the heart of their queries
    query_bodies = {
        "cpu":"increase(container_cpu_usage_seconds_total",
        "mem":"max_over_time(container_memory_working_set_bytes"
    }

    # check if user input resource is in known resources
    resources = query_bodies.keys()
    if resource not in resources:
        raise ValueError(f'resouce "{resource}" must be within one of the following resources:\n{resources}')

    # get all the pieces necessary to assemble the query
    offset = calculate_offset(start, duration)
    duration = delta_to_time_str(timedelta(seconds=duration))
    suffix = '{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    prefix = 'sum by (node, pod) ('

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
            User Functions
---------------------------------------
'''
# generate random values between run_start and some end time, put into duration1
def insert_rand_refresh_col(df, refresh_title):
    duration_seconds = df['runtime']
    # generate random values between 45sec and half of the duration
    # df[refresh_title] = duration_seconds.apply(lambda time: random.randint(45, time // 2) if time // 2 >= 45 else time)

    # generate random values between 45sec and 5min
    df[refresh_title] = duration_seconds.apply(lambda time: random.randint(45, 300) if time >= 300 else time)

    return df


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

# initialize metrics columns if they aren't already
# metrics = ['cpu_total', 'mem_total', 'cpu_t1', 'mem_t1', 'cpu_t2', 'mem_t2']
metrics = ['cpu_total', 'mem_total']
for metric in metrics:
    if metric not in training_data.columns:
        training_data[metric] = None

# query total performance data
# cpu columns
training_data = insert_column(training_data, "cpu", "cpu_total", 'runtime', n_rows)
# training_data = insert_column(training_data, "cpu", "cpu_t1", 'duration_t1', n_rows)
# training_data = insert_column(training_data, "cpu", "cpu_t2", 'duration_t2', n_rows)
# memory columns
training_data = insert_column(training_data, "mem", "mem_total", 'runtime', n_rows)
# training_data = insert_column(training_data, "mem", "mem_t1", 'duration_t1', n_rows)
# training_data = insert_column(training_data, "mem", "mem_t2", 'duration_t2', n_rows)

# print and write updated df to a csv file
print("\n"*5, training_data)
training_data.to_csv(csv_file)






# Todo: 
# add more columns / metrics:
    # IO information
    # network info



'''
======================================================
For inserting another duration and more metric columns

Note: make sure to edit insert_rand_refresh_col() to 
be within the time range you want it to be.
======================================================
'''
'''
# add new columns and insert duration col
# training_data['duration_t3'] = None
# training_data['cpu_t3'] = None
# training_data['mem_t3'] = None
# training_data = insert_rand_refresh_col(training_data, "duration_t3")

training_data = insert_column(training_data, "cpu", "cpu_t3", 'duration_t3', n_rows)
training_data = insert_column(training_data, "mem", "mem_t3", 'duration_t3', n_rows)
'''

