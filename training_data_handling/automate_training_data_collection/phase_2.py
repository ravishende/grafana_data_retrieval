import pandas as pd
import shutil
import ast
import random
from datetime import datetime
from workflow_files import phase2_files

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


'''
==========================
    Helper functions
==========================
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
            start_dt = datetime.strptime(start, format_str)
            stop_dt = datetime.strptime(stop, format_str)
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


def drop_columns(df, columns_to_drop):
    df = df.drop(columns=columns_to_drop)
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
        df[refresh_title] = duration_seconds.apply(lambda time: random.randint(45, time) if time > 45 else time)
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
        duration_title = "duration_t" + str(insert_method + 1)
        df = _insert_rand_refresh_col(df, duration_title, method=insert_method)

    return df


drop_cols_1 = [
    "path",
    # "time_scraped", # if it's there
    "extent_fmt",
    "dz",
    "fire_grid",
    "output",
    "resolution",
    "resolution_units",
    "run_binary",
    "seed",
    "timestep",
    "topo"
]


'''
======================
    Main Program
======================
'''

# get df from csv file
ids_included_df = pd.read_csv("csvs/unfiltered.csv", index_col=0)

# 4. calculate area and runtime
calculated_df = add_area_and_runtime(ids_included_df)

# 5. drop drop_cols_1
filtered_df = drop_columns(calculated_df, drop_cols_1)

# 6. add duration_t1, duration_t2 columns
num_duration_cols = 3  # number of duration columns to insert and query for (doesn't include "runtime")
preprocessed_df = insert_n_duration_columns(filtered_df, num_duration_cols, single_method=False)

# save preprocessed_df to file
preprocessed_df.to_csv(phase2_files['write'])
print(preprocessed_df)

