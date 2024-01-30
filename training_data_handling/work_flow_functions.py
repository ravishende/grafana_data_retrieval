import pandas as pd
import ast

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
def get_successful_runs(runs_df, reset_index=True):
    # get a df with only the successful runs
    successful_runs = df[(df["ens_status"]=="Done") & (df["run_status"]=="Done")]
    # if requested, reset the indices to 0 through end of new df after selection
    if reset_index:
        successful_runs = successful_runs.reset_index(drop=True)
    return successful_runs


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

def drop_columns(df, columns_to_drop):
    df = df.drop(columns=columns_to_drop)
    return df