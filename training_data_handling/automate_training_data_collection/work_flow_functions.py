import os
from multiprocessing.sharedctypes import Value
from workflow_files import MAIN_FILES, PHASE_1_FILES, PHASE_2_FILES, PHASE_3_FILES, PHASE_4_FILES, NUM_DURATION_COLS_FILE

# constants - DO NOT EDIT
NUM_PHASES = 4


# Given contents (a list to write to the file),
# Writes contents to a file. Each element is written on a new line.
# If txt_file does not exist, it is created.
def _write_txt_file(txt_file, contents):
    with open(txt_file, "w") as file:  # Open the file in append mode ('a')
        for entry in contents:
            file.write(entry + "\n")  # Write each entry on a new line

# Given a file path (directories/name), if the file exists, do nothing, otherwise, create the file
def _initialize_file(file_path):
    # Extract the directory path from the file name
    directory = os.path.dirname(file_path)
    
    # If the directory does not exist and the file is in a directory, create it
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        # If the file does not exist, create it
        with open(file_path, 'w') as file:
            return # we only want to create the file, not write to it.

# make sure all necessary files exist
def initialize_files():
    all_files_dicts = [MAIN_FILES, PHASE_1_FILES, PHASE_2_FILES, PHASE_3_FILES, PHASE_4_FILES]
    # get paths to all files so that they can be initialized
    all_file_paths = [NUM_DURATION_COLS_FILE]
    for file_dict in all_files_dicts:
        all_file_paths += file_dict.values()
    # initialize all files
    for file_path in all_file_paths:
        _initialize_file(file_path)

# given a filename of a txt file, line number, and message, update the line at line_number to be "message", ending in a new line
def _update_txt_line(file_name, line_number, message):
     # read file, then update the phase line to be message with a \n
    try:
        lines = []
        # Read the file's current contents
        with open(file_name, 'r') as file:
            lines = file.readlines()
        
        # update the file's contents
        lines[line_number] = f"{message}\n"
        with open(file_name, 'w') as file:
            file.writelines(lines)
    except Exception as e:
        raise ValueError(f"An unexpected error occurred: {e}")
        

# given a file_name and line_number, return the contents of the line (without any trailing whitespace or \n at the end of the line)
def _read_txt_line(file_name, line_number):
     # read file, then check the line's progress
    try:
        # Read the file's current contents
        with open(file_name, 'r') as file:
            lines = file.readlines()
            line_content = lines[line_number].strip()  # Remove leading/trailing whitespace and newline
            return line_content
    except Exception as e:
        raise ValueError(f"An unexpected error occurred: {e}")
    

# check that the input phase_number is a valid phase
def _check_phase_number(phase_number):
    # check input is a valid phase
    if phase_number not in range(1, NUM_PHASES+1):
        raise ValueError(f"phase_number ({phase_number}) must be an integer between 1 and {NUM_PHASES} inclusive\n")


# given a phase number (1 through 4): return True or False depending on whether the stater has been finished or not
def is_phase_finished(phase_number):
    # check that input is valid
    _check_phase_number(phase_number)
    # get the status of the requested phase
    line_number = phase_number-1
    phase_status = _read_txt_line(MAIN_FILES["phases_progress"], line_number)
    # return True if phase_status is 'T', or False otherwise
    return phase_status == 'T'


# given a phase number (1 through 4): set the phase's 'finished' progress to be True
def set_phase_finished(phase_number):
    # check input is a valid phase
    _check_phase_number(phase_number)
    
    # make sure phases aren't being set incorrecly out of order
    if phase_number > 1:
        if not is_phase_finished(phase_number-1):
            raise ValueError(f"Phase {phase_number} cannot be set as finished because phase {phase_number-1} is still unfinished. Phases must be done sequentially.")

    # write the line number to be "T" (True) at the line corresponding to the phase_number in the phases_progress txt file
    line_num = phase_number-1
    _update_txt_line(file_name=MAIN_FILES['phases_progress'], line_number=line_num, message="T")


# given a phase number (1 through 4): set the phase's 'finished' progress to be False
def set_phase_unfinished(phase_number):
    # check input is a valid phase
    _check_phase_number(phase_number)
    # make sure phases aren't being set incorrecly out of order
    if phase_number < 4:
        if is_phase_finished(phase_number+1):
            raise ValueError(f"Phase {phase_number} cannot be set as unfinished because phase {phase_number+1} is finished. Phases must be done sequentially.")
    
    # write the line number to be "F" (False) at the line corresponding to the phase_number in the phases_progress txt file
    line_num = phase_number-1
    _update_txt_line(file_name=MAIN_FILES['phases_progress'], line_number=line_num, message="F")


# given the total number of phases, set the progress of all phases to False
def reset_phases_progress():
    # if file does not have the proper number of phases represented, 
    if len(MAIN_FILES['phases_progress']) != NUM_PHASES+1:
        # rewrite file, setting all phases to incomplete
        _write_txt_file(
            txt_file=MAIN_FILES['phases_progress'], 
            contents=['F' for _ in range(NUM_PHASES)])
        return
    # otherwise, set all phases to False
    for phase_number in range(1, NUM_PHASES+1):
        set_phase_unfinished(phase_number)
    






# Old functions that have now been moved into individual Phase clases in phase_1.py, phase_2.py, phase_3.py and phase_4.py
"""
import pandas as pd
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("work_flow_functions.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from query_resources import query_and_insert_columns
from resource_json_summation import update_columns


'''
========================
        Phase 1
========================
'''

'''
------
step 1 - get successful bp3d runs
------
'''
# given a dataframe of runs with ens_status and run_status columns,
# return a new dataframe with only the successful runs 
def get_successful_runs(df, reset_index=True):
    # get a df with only the successful runs
    successful_runs = df[(df["ens_status"]=="Done") & (df["run_status"]=="Done")]
    # if requested, reset the indices to 0 through end of new df after selection
    if reset_index:
        successful_runs = successful_runs.reset_index(drop=True)
    return successful_runs


'''
------
step 2 - collect runs
------
'''

def collect_runs(df):
    pass

'''
------
step 3 - add in ensemble_uuid
------
'''
# given a dataframe of successful runs (that have run_uuid, ensemble_uuid) and a dataframe of runs collected (with information on each run)
# return a dataframe of the two merged to include all information on the run and id columns
def add_id_cols(successful_runs_df, collected_runs_df):
    successful_runs_subset = successful_runs_df[['ensemble_uuid', 'run_uuid']]
    merged_df = pd.merge(collected_runs_df, successful_runs_subset, left_index=True, right_index=True)
    return merged_df

'''
========================
        Phase 2
========================
'''


'''
------
step 4 - calculate area and runtime
------
'''

def _calculate_area(corners_list):
    # where p1 in the bottom left = (x1,y1) and p2 in the bottom left = (x2,y2)
    # corners_list is of the form [x1, y1,, x2, y2]
    corners_list = ast.literal_eval(corners_list) # converting string to list
    x1, y1, x2, y2 = float(corners_list[0]), float(corners_list[1]), float(corners_list[2]), float(corners_list[3])
    x_length = abs(x2-x1)
    y_length = abs(y2-y1)
    area = x_length * y_length
    return area

def _calculate_runtime(start, stop):
    # there are two slightly different ways that time strings are represented. Try the other if the first doesn't work.
    # get start and stop down to the second, no fractional seconds.
    start = start[0:start.find(".")]
    stop = stop[0:stop.find(".")]
    
    # get start and stop as datetimes
    parsing_successful = False
    format_strings = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]
    for format_str in format_strings:
        try:
            start_dt = datetime.strptime(start, format_string)
            stop_dt = datetime.strptime(stop, format_string)
            # if parsing is successful, break the loop
            parsing_successful = True
            break
        except ValueError:
            continue  # if parsing failed, try next format string
    if not parsing_successful:
        raise ValueError("Time format not recognized")

    # find the difference between stop and start for the runtime
    runtime_delta = stop_dt - start_dt
    return runtime_delta.total_seconds()


# given a dataframe with 'extent', 'start', and 'stop' columns, 
# return a df with added 'area' and 'runtime' columns
def add_area_and_runtime(df):
    df['area'] = df['extent'].apply(_calculate_area)
    df['runtime'] = df.apply(lambda row: _calculate_runtime(row['start'], row['stop']), axis=1)
    return df


'''
------
step 5 - drop unnecessary columns
------
'''

def drop_columns(df, columns_to_drop, reset_index=True):
    df = df.drop(columns=columns_to_drop)
    if reset_index:
        df = df.reset_index(drop=True)
    return df


'''
------
step 6 - add duration_t1, duration_t2 columns
------
'''

# generate random values between run_start and some end time, put into duration1
def _insert_rand_refresh_col(df, refresh_title, method=0):
    duration_seconds = df['runtime']
    if method == 0:
        # generate random values between 45sec and 5min
        df[refresh_title] = duration_seconds.apply(lambda time: random.randint(45, 300) if time >= 300 else time)
    elif method == 1:
        # generate random values between 45sec and half of the duration
        df[refresh_title] = duration_seconds.apply(lambda time: random.randint(45, time // 2) if time // 2 >= 45 else time)
    elif method == 2:
        # generate random values between 45sec and the full duration
        df[refresh_title] = duration_seconds.apply(lambda time: random.randint(45, time))
    else:
        raise ValueError("method must be: 0, 1, or 2")
    return df


NUM_DURATION_COLS = 0

def _get_num_duration_cols():
    return NUM_DURATION_COLS

# given a dataframe and number of duration columns to insert, (also single_method, which is either False or some int between 0 and 2)
# return an updated dataframe with an added n duration columns of various insert methods
def insert_n_duration_columns(df, n, single_method=False):
    # initialize NUM_DURATION_COLS to be n for later steps
    global NUM_DURATION_COLS
    NUM_DURATION_COLS = n

    num_insert_methods = 3
    # warn the user if they are expecting more insert methods than are available in _insert_rand_refresh_col
    if n > num_insert_methods and not single_method:
        warnings.warn("There are more columns requested than insert methods defined. Repeating the last method after other methods are used.")
    for i in range(0, n):
        # get the insert method
        if single_method:
            insert_method = single_method
        else:
            insert_method = i
            if insert_method >= num_insert_methods:
                insert_method = num_insert_methods - 1
        # assemble the duration_title
        duration_title = "duration_t" + insert_method
        df = _insert_rand_refresh_col(df, duration_title, method=insert_method)



'''
========================
        Phase 3
========================
'''


'''
------
step 7 -  query resource metrics (metrics total, t1, t2)
------
'''

# get the static and non_static metrics lists
def _get_metrics():
    # define the metrics to be queried
    # list of all metrics you can query (with query_and_insert_columns())
    all_metrics = [
        "cpu_usage",
        "mem_usage",
        "cpu_request",
        "mem_request",
        "transmitted_packets",
        "received_packets",
        "transmitted_bandwidth",
        "received_bandwidth"
        ]
    # metrics that don't change over a run
    static_metrics = ["cpu_request", "mem_request"]
    # metrics that do change over a run
    non_static_metrics = [metric for metric in all_metrics if metric not in STATIC_METRICS]

    return static_metrics, non_static_metrics


def _get_metric_column_names():
    # get column names
    col_names_static = static_metrics
    col_names_total = [name + "_total" for name in non_static_metrics]
    num_duration_cols = _get_num_duration_cols()

    # get col_names_t1, col_names_t2, etc. in a list called col_names_by_time
    col_names_by_time = []
    for i in range(1, num_duration_cols):
        col_names_t_i = [name + "_t" + i for name in non_static_metrics]
        col_names_by_time.append(col_names_t_i)


# given: 
    # df - a dataframe 
    # batch_size - number of rows to query at a time until the df is filled out
    # temporary_save_file - name of a csv file to save df to after each big insert in case the program is stopped
# query all important metrics, saving to the temporary_save_file after inserting columns of the same duration column.
    # Note: this function assumes the total duration column is "runtime" and duration columns 
    # are in the form "duration_t{N}" where {N} is an int from 1 to num_duration_cols inclusive
# return the updated dataframe with all columns queried
def query_metrics(df, batch_size, temporary_save_file):
    # get metrics lists and number of duration columns
    static_metrics, non_static_metrics = _get_metrics()
    num_duration_cols = _get_num_duration_cols()

    # get duration column names
    duration_col_names = ["duration_t" + num for num in range(1,num_duration_cols+1)]
    duration_col_total = "runtime"

    # get metric column names
    col_names_static, col_names_total, col_names_by_time = _get_metric_column_names()

    # collect all metric column names and initialize them in the dataframe if they aren't already
    all_col_names = col_names_static + col_names_total + [col_names_t_i for col_names_t_i in col_names_by_time]
    for column in all_column_names:
        if column not in df.columns:
            df[column] = None

    # while there are still unqueried rows, keep querying batch_size rows at a time
    while df[non_static_metrics[0]].iloc[len(df)-1].isna():
        # query and insert static and total columns
        df = query_and_insert_columns(df, static_metrics, col_names_static, duration_col_total, batch_size)
        df.to_csv(temporary_save_file)
        df = query_and_insert_columns(df, non_static_metrics, col_names_total, duration_col_total, batch_size)
        df.to_csv(temporary_save_file)
        # query and insert duration_t_i columns
        for i, col_names_t_i in enumerate(col_names_by_time):
            df = query_and_insert_columns(df, non_static_metrics, col_names_t_i, duration_cols[i], batch_size)
            df.to_csv(temporary_save_file)

    return df



'''
========================
        Phase 4
========================
'''

'''
------
step 8 - sum over json to get floats for resource metrics
------
'''

# given a df with a column titled "ensemble_uuid" and queried columns with json-like data
# return a df with all queried columns updated to be a single float value
def update_queried_cols(df):
    # get metrics lists
    static_metrics, non_static_metrics = _get_metrics()
    ensemble_col = "ensemble_uuid"
    
    # update columns - sum non_static columns, get values for static columns
    df = update_columns(df, non_static_metrics, ensemble_col, static=False)
    df = update_columns(df, static_metrics, ensemble_col, static=True)

    return df


'''
------
step 9 - add in percent columns
------
'''

def add_percent_columns(df):
    # get number of duration columns
    num_duration_cols = _get_num_duration_cols()

    # metrics lists that will be used to get/calculate percentages
    percent_metrics = ["cpu_request_%", "mem_request_%"]  # these do not exist yet - the columns for these metrics will be calculated
    numerator_metrics = ["cpu_usage", "mem_usage"]
    denominator_metrics = ["cpu_request", "mem_request"]

    # insert percentage columns as df[percent_metric_col] = 100 * df[numerator_metric_col] / df[denominator_metric_col]
    df = insert_percent_cols(
        df, percent_metrics, numerator_metrics, denominator_metrics, 
        num_duration_cols, static_metrics)

    return df


'''
-------
step 10 - drop rows with zeros in cpu & mem usage total
-------
'''

# given a dataframe with the columns "cpu_usage_total" and "mem_usage_total", drop all rows where those columns are 0
# return the updated dataframe
def drop_zero_cpu_mem(df, reset_index=True):
    zero_mask = (summed['cpu_usage_total'] == 0) | (summed['mem_usage_total'] == 0)
    non_zeros = summed[~zero_mask]
    if reset_index:
        non_zeros = non_zeros.reset_index(drop=True)


'''
-------
step 11 - add ratio cols for duration_t_i columns and drop numerator columns
-------
'''

# given an i (1 through num_duration_cols inclusive), 
# return 
    # insert_ratio_cols - a list of ratio column names at i to be inserted
    # numerator_cols - a list of numerator column names at i to use for calculation (that already exist)
    # duration_col - a column name of the duration column at i
def _get_ratio_components(i):
    if i < 1:
        warnings.warn("i should only be 1 through num_duration_cols inclusive. Otherwise, there may be unexpected behaviors")

    # get non static metrics which will be used to find numerator columns
    _, non_static_metrics = _get_metrics()

    # get numerator column names, new insert column names, and duration column name
    numerator_cols = [f"{metric}_t{i}" for metric in non_static_metrics]
    insert_ratio_cols = [f"{name}_ratio" for name in numerator_cols]
    duration_col = f"duration_t{i}"

    return insert_ratio_cols, numerator_cols, duration_col


# given a dataframe, return the updated dataframe 
# with new columns inserted as a ratio of numerator_col/duration_col
def insert_ratio_columns(df, drop_numerators=True, reset_index=True):
    # handle improper user input
    if reset_index and not drop_numerators:
        raise ValueError("reset_index can only be True if drop_numerators is also True")

    # get number of duration columns
    num_duration_cols = _get_num_duration_cols()

    # get column names, then calculate and insert ratio columns
    for i in range(1, num_duration_cols+1):
        # get column names for ratio cols to be inserted, numerator cols, duration (denominator) col
        cols_to_insert, numerator_cols, duration_col = _get_ratio_components(i)

        # calculate and insert ratio columns
        for insert_col, numerator_col in zip(cols_to_insert, numerator_cols):
            df[insert_col] = df[numerator_col].astype(float) / df[duration_col].astype(float)

        # drop numerator columns if requested
        if drop_numerators:
            df.drop(columns=numerator_cols)
            # reset index if requested
            if reset_index:
                df = df.reset_index(drop=True)

    return df


"""