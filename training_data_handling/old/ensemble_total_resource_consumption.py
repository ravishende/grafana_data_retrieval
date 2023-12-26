import pandas as pd
from datetime import datetime, timedelta
from termcolor import colored
import random
# get set up to be able to import files from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("ensemble_total_resource_metrics.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.querying import query_data
from helpers.printing import print_title

# Settings - You can edit these, especially NUM_ROWS, which is how many rows to generate per run
pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
csv_file = 'csv_files/' #p2_training_data.csv
NUM_ROWS = 1000
NAMESPACE = 'wifire-quicfire'

# For printing rows. Do not edit.
CURRENT_ROW = 1

'''
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------
NOTE:
This file currently reads in a csv called 'p2_training_data.csv', 
which contains all the training data from BP3D runs' inputs as well as cpu_usage_total and mem_usage total for the entire run. 
On top of this, it also contains a row for duration1 which is from the start of the run until some random refresh time. Then it has
cpu_t1 and mem_t1 which are the cpu and memory usage up until this refresh time. It does the same with duration2 and cpu_t2 and mem_t2, 
although duration_2 is between 45 seconds and 300 (5minutes)

Running this appends NUM_ROWS values 
for those two columns (cpu_t2 and mem_t2) to what has already been queried for, filling out more and 
more of the datapoints. 

All that needs to be done is select NUM_ROWS to be the value you would like and then run the file.

Something to Note:
 - duration1 is a random number between 45sec and the length of the run. If a row has a duration column with a number less than 45,
   that means the total length of the run is less than 45 seconds. Duration would then be the total runtime.
 - it may be good to exclude those values in training a model, since the cpu_t1 and mem_t1 will match cpu_total and mem_total.
 - the duration starts at 45 seconds for healthy runs, since there is a high likelihood of no data if querying less than 45 seconds.
    - below is the table showing for x second duration for 100 rows queried, how many missing datapoints there were in cpu_t1 and mem_t1 combined:
        - So, for 40 seconds and 100 runs, if there are 4 cpu_t1 no_data points and 6 mem_t1 no_data points, it will be "40: 10 / (2*100)"). It is *2 since no_data is summed for cpu and mem for those 100 rows.
        
        # seconds : no data / total runs 
        # 31:       37 / (100*2)
        # 35:       19 / (100*2)
        # 40:       11 / (100*2)
        # 45:       10 / (100*2)
        # 60:        8 / (100*2)
        # 120:       8 / (100*2)
        # 3000:      6 / (100*2)
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
    if isinstance(time, pd.Timestamp):
        return time.to_pydatetime(warn=False)
     # check if time is a datetime object
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


'''
Given a resource ("cpu", "mem", "io", or "network"), duration (e.g. "2h15m") and offset (e.g. "1d")
return the corresponding query (string) for that resource
'''
def get_resource_query(resource, duration, offset):
    query_prefix = 'sum by (node, pod) (increase('
    query_suffix = '{namespace="' + NAMESPACE + '"}[' + str(duration) + '] offset ' + str(offset) + '))'
    resource_queries = {
        'cpu': query_prefix + 'container_cpu_usage_seconds_total' + query_suffix,
        'mem':query_prefix + 'container_memory_working_set_bytes' + query_suffix,
        'io':query_prefix + '________' + query_suffix,
        'network':query_prefix + '________' + query_suffix
    }
    return resource_queries[resource]



'''
Use:
   query for resource usage of a run over a period of time and print updates on rows completed
Parameters:
 - row: row in DataFrame to insert data for
 - resource: one of "cpu", "mem", "io", or "network"
 - run_start: datetime of the time the run started
 - run_stop: datetime of the time to stop querying
 - row_index: (for printing) row in DataFrame this is querying for
Returns:
 - a result_list of queried data in the form [{'metric':..., 'value':...}, {'metric':..., 'value':...}, ...]
'''
def get_data(resource, run_start, run_stop, row_index):
    # get start and stop as datetime objects
    # start = datetime_ify(run_start)
    # stop = datetime_ify(run_stop)

    # assemble query
    duration, offset = calculate_duration_and_offset(run_start, run_stop)
    resource_query = get_resource_query(resource, duration, offset)
    
    # query data
    resource_data = query_data(resource_query)
    
    # print row information
    global CURRENT_ROW
    global NUM_ROWS
    progress_message = "Row complete: " +  str(CURRENT_ROW) + " / " +  str(NUM_ROWS)
    print(colored(progress_message, "green"))
    print("row index:", row_index)
    CURRENT_ROW += 1

    return resource_data




'''
---------------------------------------
            User Function
---------------------------------------
'''

# generate random values between run_start and run_end/2, put into duration1
def insert_rand_refresh_col(df, refresh_title):
    # convert start and stop times to datetime
    df['start'] = df['start'].apply(datetime_ify)
    df['stop'] = df['stop'].apply(datetime_ify)

    # calculate duration in seconds
    # duration_seconds = (df['stop'] - df['start']).dt.total_seconds()
    duration_seconds = df['runtime']

    # generate random values between 0 and half of the duration
    # df[refresh_title] = duration_seconds.apply(lambda x: random.randint(45, x // 2) if x // 2 >= 45 else x)
    df[refresh_title] = duration_seconds.apply(lambda x: random.randint(45, 300) if x >= 300 else x)

    return df

# # used for testing when it is useful to have earliest refresh available
# def insert_incremental_refresh_col(df, refresh_title):
#     df[refresh_title] = df.index+45
#     return df


# 
def insert_column(df, col_to_insert, resource, run_stop=None, run_duration=None):
    # find the first row index where the column to insert has no data
    global NUM_ROWS
    global CURRENT_ROW
    start_row = df[col_to_insert].isna().idxmax()
    end_row = start_row + NUM_ROWS - 1
    
    # make sure only one from run_stop and run_duration are specified
    if (run_stop is None and run_duration is None) or (run_stop is not None and run_duration is not None):
        raise ValueError("Exactly one of run_stop or run_duration must be specified")

    # make sure you don't try to query past the end of the dataframe
    last_index = len(df) - 1
    if end_row > last_index:
        end_row = last_index
        NUM_ROWS = end_row - start_row + 1
        print(colored("\nEnd row is greater than last index. Only generating to last index.", "yellow"))
    
    # if no na values, there is nothing to generate.
    if start_row == 0 and not pd.isna(df[col_to_insert].iloc[0]):
        print(colored("\nNo NaN values to fill in specified column.", "yellow"))
        return df

    # convert run_start from a series of strings to a series of datetimes
    run_start = df['start'].apply(lambda x: datetime_ify(x))
    # if run_duration was given, calculate new run_stop from it.
    if run_stop is None:
        # duration_delta = run_duration.apply(lambda x: timedelta(seconds=int(x)))
        duration_delta = run_duration.apply(lambda x: timedelta(seconds=x))
        run_stop = run_start + duration_delta
    else:
        # convert run_stop to datetimes
        run_stop = run_stop.apply(lambda x: datetime_ify(x))

    # insert data into the column at specified rows
    df[col_to_insert] = df.apply(
        # Note: one of run_stop or run_duration must be None - already checked at start of function
        lambda row: get_data(resource, run_start[row.name], run_stop[row.name], row.name) \
        if start_row <= row.name <= end_row else row[col_to_insert], axis=1)

    # reset CURRENT_ROW for pinting in case the function is called again
    CURRENT_ROW = 1

    return df




'''
---------------------------------------
            Main Program
---------------------------------------
'''

# get the csv file as a pandas dataframe
print("Reading csv...")
training_data = pd.read_csv(csv_file, index_col=0)
duration_col = training_data['duration2']

# generate cpu data at refresh time
print_title("Generating CPU Usage")
resource_col = "cpu_t2"
resource = "cpu"
training_data = insert_column(training_data, resource_col, resource, run_duration=duration_col)

# generate mem data at refresh time
print_title("Generating Memory Usage")
resource_col = "mem_t2"
resource = "mem"
training_data = insert_column(training_data, resource_col, resource, run_duration=duration_col)


# print updated df and write it to a csv file
print("\n"*5, training_data)
training_data.to_csv(csv_file)





'''
# add new columns and insert duration col
training_data['duration3'] = None
training_data['cpu_t3'] = None
training_data['mem_t3'] = None
training_data = insert_rand_refresh_col(training_data, "duration3")


def print_sub_n_durations(n, duration_title, stop_index):
    for index, row in training_data.iterrows():
    if(index > stop_index): break
    if row[duration_title] < n:
        print(colored(index, "green"))
        print(row[duration_title], "||", row["cpu_t1"], "||", row["mem_t1"])
        print(row['runtime'], "||", row['start'], "||", row["stop"])
'''

# Todo: 
# get cpu_usage and mem_usage columns to a single number
    # filter cpu_usage and mem_usage columns to only include pods in the correct ensemble. 
    # then sum over all pods to get total cpu_usage for the ensemble during the runtime.
# add more columns:
    # IO information - total, at t1, and at t2
    # network info - total, at t1, and at t2
    # add t3 - and respective columns
