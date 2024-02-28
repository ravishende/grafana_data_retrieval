import os
import s3fs
import zarr
import json
import math
import pickle
import pandas as pd
from tqdm import tqdm
from pprint import pprint
from termcolor import colored
from datetime import datetime
from dotenv import load_dotenv
from workflow_files import phase1_files

# settings
pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)

'''
=========================
Phase 1:  Collecting Runs
=========================
1. get successful bp3d runs from bp3d-runs
    - run_selection.py
2. collect runs from successful bp3d runs
    - gather.ipynb in collect_runs/

FINISH:
save df to a file
'''



# Given contents (a list to write to the file),
# Writes contents to a file. Each element is written on a new line.
# If txt_file does not exist, it is created.
def write_txt_file(txt_file, contents):
    with open(txt_file, "w") as file:  # Open the file in append mode ('a')
        for entry in contents:
            file.write(entry + "\n")  # Write each entry on a new line


# Given paths_batch (a list of paths to append to the file),
# appends a batch of entries to txt_file. Each entry is written on a new line.
# If txt_file does not exist, it is created.
def append_txt_file(txt_file, batch):
    with open(txt_file, "a") as file:  # Open the file in append mode ('a')
        for entry in batch:
            file.write(entry + "\n")  # Write each entry on a new line


# given the path to a .txt file, return a list where each line of the 
# txt file is an element in the list
def read_txt_file(txt_file):
    contents = []
    with open(txt_file,"r") as f:
        contents = f.read().splitlines() 
    return contents


# given the fs and a subdirectory, return all of the run simulation paths
def _get_sim_paths(fs, subdir):
    sim_paths = []
    paths = fs.ls(subdir)
    for path in paths:
        if "run_" in path:
            sim_paths.append(path)
    return sim_paths


# given an item type ('paths' or 'path_directories')
# return all of the previously gathered items of that type.
# note: only works if workflow_files.phase1_files is set up so
# that item_title and "old"+item_title are both keys of phase1_files
def _get_gathered_items(item_title):
    # make sure user input is valid
    valid_item_titles = ['path_directories', 'paths']
    if item_title not in valid_item_titles:
        raise ValueError(f'item_title must be one of the following: {valid_item_titles}')
    
    # if no previously gathered items, get previously gathered items from past gatherings
    gathered_items = read_txt_file(phase1_files[item_title])
    if len(gathered_items) == 0:
            # update gathered_items to contain the old gathered items
            old_item_title = 'old_' + item_title
            gathered_items = read_txt_file(phase1_files[old_item_title])
            append_txt_file(phase1_files[item_title], gathered_items)
    return gathered_items


# returns a list of attributes of a run to keep
def _get_keep_attributes():
    keep_attributes = [
        'canopy_moisture',
        'extent',
        'run_end',
        'run_max_mem_rss_bytes',
        'run_start',
        'sim_time',
        'surface_moisture',
        'threads',
        'wind_direction',
        'wind_speed'
    ]
    return keep_attributes


# authenticate and return the file system and bucket containing directories for paths
def _get_fs_and_bucket():
    # get login details from .env file
    if not load_dotenv():
        raise EnvironmentError("Failed to load the .env file. This file should contain the ACCESS_KEY and SECRET_KEY")
    endpoint = 'https://wifire-data.sdsc.edu:9000'
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")

    # login and get fs (file system) and bucket
    fs = s3fs.S3FileSystem(key=access_key,
        secret=secret_key,
        client_kwargs={
            'endpoint_url': endpoint,
            'verify': False
        },
        skip_instance_cache=False
    )
    bucket = 'burnpro3d/d'

    print(colored("\n\nsuccessfully authenticated", "green"))
    return fs, bucket


# given a list of ungathered directories as well as the file system, 
# return all the paths from those directories.
def _get_paths_from_directories(directories, fs):
    paths = []
    for directory in tqdm(directories):
        subdirectories = fs.ls(directory)
        for subdir in subdirectories:
            paths += _get_sim_paths(fs, subdir)
    # write newly gathered directories to a file so they don't ever have to be regenerated
    append_txt_file(phase1_files['path_directories'], directories)
    return paths


# batch size is a number of paths per batch to get
def gather_all_paths(batch_size=None):
    # authenticate and get file system and bucket containing directories for paths
    fs, bucket = _get_fs_and_bucket()
    
    # get all directories and previously gathered directories
    directories = fs.ls(bucket)
    gathered_directories = _get_gathered_items("path_directories")
    
    # get list of directories that have not been gathered
    ungathered_directories = [d for d in directories if d not in gathered_directories]
    # the last gathered directory may have new subdirectories. Add it into ungathered_directories to get new subdirs
    ungathered_directories = [gathered_directories[-1]] + ungathered_directories
    # get rid of duplicates in ungathered_directories (from adding in gathered_directories[-1])
    ungathered_directories = list(set(ungathered_directories))
    
    # intialize a list to hold all simulation paths
    simulation_paths_list = _get_gathered_items("paths")
    # handle if no new directories
    if ungathered_directories == [gathered_directories[-1]]:
        return simulation_paths_list
    
    # start gathering directories
    print(f"{len(gathered_directories)} directories have already been gathered. \
        Gathering paths for the remaining {len(ungathered_directories)}.") 
        # note: if you were to calculate total based on these 2 values, you'd notice that len(directories) = total -1. This is because
        # we added gathered_directories[-1] to ungathered_directories. We will also have to run .unique() on paths because of this

    # if we're not using batches, run everything at once
    if batch_size is None:
        new_sim_paths_list = _get_paths_from_directories(ungathered_directories, fs)
        append_txt_file(phase1_files['paths'], new_sim_paths_list)
        return simulation_paths_list + new_sim_paths_list

    # collect runs in batches
    num_batches = math.ceil(len(ungathered_directories)/batch_size)
    for i in range(0, num_batches):
        # get end index for batch. Shouldn't change because 
        end_index = batch_size
        # if this is the last iteration, generate paths until the end of ungathered_directories
        if i == num_batches-1:
            end_index = len(ungathered_directories)

        # get simulation paths for this batch
        print(f"\nGetting paths for {end_index} / {len(ungathered_directories)} directories left.", colored(f"Batch {i+1}/{num_batches}", "magenta"))
        sim_paths_batch = _get_paths_from_directories(ungathered_directories[:end_index], fs)
        # update all simulation paths and remove newly gathered directories from ungathered directories
        simulation_paths_list += sim_paths_batch
        ungathered_directories = ungathered_directories[end_index:]

        # append newly collected paths to the paths.txt file
        append_txt_file(phase1_files['paths'], sim_paths_batch)
    
    return simulation_paths_list

# get the run_uuid (str) from a path (str)
def _run_id_from_path(path):
    run_uuid = path.split('/')[-1]
    return run_uuid


# given a df with a 'path' column, add a new column called 'run_uuid' 
# which gets the run_uuid from the path. Return the new df.
def add_run_uuid_col(df):
    path_col = df['path']
    df['run_uuid'] = df['path'].apply(_run_id_from_path)
    return df


# Given a paths list and a method of dropping old paths ("txt" or "training_data")
# Return a new list of paths that only contains new paths (paths not in old paths)
# Note: method="txt" should be used by default, unless there is no old_paths.txt file
def _drop_old_paths(paths, method="txt"):
    # use old_paths.txt file to subtract all old paths from current paths file
    if method == "txt":
        # get a list of old paths
        old_paths = read_txt_file(phase1_files['old_paths'])
        # create a new list of paths that only contains paths not in old_paths
        new_paths = [p for p in paths if p not in old_paths]
        # save new paths to a file
        write_txt_file(phase1_files['new_paths'], new_paths)
        return new_paths

    # get run_uuids from paths, then for each run_uuid in training_data, get rid of that path
    if method == "training_data":
        # get list of run_uuids from training_data
        training_data = pd.read_csv(phase1_files['training_data'])
        existing_uuids = training_data['run_uuid'].to_list()
        # Use the _run_id_from_path function to extract run_uuid from each path
        new_paths = [p for p in paths if _run_id_from_path(p) not in existing_uuids]
        # save new paths to a file
        write_txt_file(phase1_files['new_paths'], new_paths)
        return new_paths
    
    # handle incorrect method user error
    raise ValueError("method must be either 'txt' or 'training_data'")


# given a start and stop index and a list of paths,
# return a df of runs in the section of paths[start:stop]
def get_df_chunk(start, stop, paths):
    # initialize a list of paths that cause filenotfound errors
    bad_paths = []
    # variable to count the amount of runs missing data (columns)
    runs_missing_data = 0

    # don't try to access out of bounds of path
    if stop > len(paths):
        print(colored("stop index out of bounds - updating to be len(paths)", "yellow"))
        stop = len(paths)
    if start >= len(paths):
        raise ValueError("start cannot be greater than or equal to len(paths)")

    print(f"Reading from line {start} to {stop}")
    # get each path in the chunk and get the run from that path
    rows = [] # list that will collect row dictionaries to be put into the df
    for path in tqdm(paths[start:stop]):
        # try to get the file from the path
        try:
            name = 'quicfire.zarr'
            with fs.open(path + '/' + name + '/.zattrs') as file:
                run_data=json.load(file)
        # if the file isn't there, append path to bad_paths
        except:
            bad_paths.append(path)
            continue

        # add all the important attributes of the run to the row
        row = run_data

        # if an attribute is not in the row, add it as None
        keep_attributes = _get_keep_attributes()
        complete_run = True
        for attr in keep_attributes:
            if attr not in run_data:
                row[attr] = None
                complete_run = False
        # increment runs_missing_data if the run has columns missing
        if not complete_run:
            runs_missing_data += 1

        # add a path column to the row, then append it to rows
        row['path'] = path
        rows.append(row)
    

    # if there are no successful rows, return an empty dataframe
    if len(rows) == 0:
        print(colored("No runs found for this batch", "red"))
        return pd.DataFrame()

    # create the df from all of the rows
    df = pd.DataFrame(rows)
    columns_to_keep = ['path'] + keep_attributes
    df = df[columns_to_keep]
    df = add_run_uuid_col(df)

    # print file not found files
    if len(bad_paths) > 0:
        print("FileNotFound Error on the following Files:")
        for file_path in bad_paths:
            print("\t" + file_path)
    print(colored(f"\nRead from {start} to {stop}\n", "green"))

    # append bad paths to files not found
    append_txt_file(phase1_files['files_not_found'], bad_paths)

    # return df
    return df


# given the simulation paths, create a df containing runs
# for all paths that have corresponding files
def get_df_from_paths(simulation_paths, batch_size=1000):
    # find out how many runs have been looked at already
    try:
        runs_df = pd.read_csv(phase1_files['runs_df'], index_col=0)
        files_not_found = read_txt_file(phase1_files['files_not_found'])
        num_gathered_runs = len(runs_df) + len(files_not_found)
    except:
        num_gathered_runs = 0

    # calculate how many batches to run
    num_batches = (len(simulation_paths) - num_gathered_runs) // batch_size
    
    # loop over unexplored simulation paths, getting df chunks for each batch
    current_batch = 0
    for start_index in range(num_gathered_runs, len(simulation_paths), batch_size):
        # print current batch
        print(colored(f"batch {current_batch}/{num_batches}:", "green"))
        current_batch += 1

        # get the stop_index, making sure not to have it larger than len(simulation_paths)
        stop_index = min(start_index + batch_size, len(simulation_paths))
        
        # get the df from the runs
        partial_runs_df = get_df_chunk(start_index, stop_index, simulation_paths)

        # save the df to a file
        if len(partial_runs_df) > 0:
            partial_runs_df.to_csv(phase1_files['runs_df'], mode='a')
            print(partial_runs_df)

    # get the total runs df and return it
    runs_df = pd.read_csv(phase1_files['runs_df'], index_col=0)
    return runs_df


# given a dataframe of runs with ens_status and run_status columns,
# return a new dataframe with only the successful runs 
def get_successful_runs(df, reset_index=True):
    # get a df with only the successful runs
    successful_runs = df[(df["ens_status"]=="Done") & (df["run_status"]=="Done")]
    # if requested, reset the indices to 0 through end of new df after selection
    if reset_index:
        successful_runs = successful_runs.reset_index(drop=True)
    return successful_runs


# get a df of path, run_uuid, where we filter new_paths, only including the paths with run_uuids that were successful.
def get_runs_to_gather_df(new_paths, successful_runs_list_df):
    # Convert new_paths list to a DataFrame
    new_paths_df = pd.DataFrame(new_paths, columns=['path'])
    # Apply _run_id_from_path function to get run_uuid
    new_paths_df['run_uuid'] = new_paths_df['path'].apply(_run_id_from_path)
    # Merge with successful_runs_list_df to filter out unsuccessful runs
    result_df = new_paths_df[new_paths_df['run_uuid'].isin(successful_runs_list_df['run_uuid'])]
    return result_df



# given a df that contains all successful runs and a df that 
def merge_dfs(runs_data_df, runs_list_df):
    # Selecting the required columns from successful_runs_list_df
    successful_runs_cols = successful_runs_list_df[['ensemble_uuid', 'run_uuid']]
    
    # Merging the dataframes on 'run_uuid' with an inner join
    merged_df = pd.merge(successful_runs_cols, all_runs_df, on='run_uuid', how='inner')
    
    return merged_df


# given a df returns an updated df with the NaN time rows removed
def remove_na_rows(df):
    # crucial columns that need to have data. If they do not, that means the run failed somewhere
    time_cols = ['run_start', 'run_end']

    # Create a new DataFrame that includes rows with NA values in 'start', 'stop', or 'runtime'
    # Then store it in a csv file called na_times
    na_mask = df[time_cols].isna()
    na_rows_df = df[na_mask.any(axis=1)]
    if len(na_rows_df)>0:
        na_rows_df.to_csv('csvs/na_times.csv', mode='a')

    # Drop columns with NA values in any of the time columns
    df = df.dropna(subset=time_cols)

    # Rename the 'run_end' column to stop and run_start column to 'start'
    df = df.rename(columns={"run_end": "stop", "run_start": "start"})
    return df


# given a df of runs, keep only the runs that aren't already in training_data
# these runs are checked by the "run_uuid" column in the df and training_data
def get_new_runs_df(df):
    training_data = pd.read_csv(phase1_files['training_data'], index_col=0)

    # find runs in df that are not already in training_data by "run_uuid"
    new_runs_mask = ~df['run_uuid'].isin(training_data['run_uuid'])
    new_runs_df = df[new_runs_mask]

    return new_runs_df



'''
======================
    Main Program
======================
'''

# gather simulation paths to be read 
simulation_paths = gather_all_paths(batch_size=5)  # for if simulation_paths are not yet fully gathered
'''
# simulation_paths = read_txt_file(phase1_files['paths'])  # for if simulation_paths are fully gathered
# new_paths = _drop_old_paths(simulation_paths, method="txt")  # for if new_paths are not yet generated
new_paths = read_txt_file(phase1_files['new_paths']). #for if new_paths are already generated
print("simulation paths length:", len(simulation_paths))
print("New paths length:", len(new_paths))

# get a df that only contains the ids and status of successful runs
runs_list_df = pd.read_csv(phase1_files['read'])
successful_runs_list_df = get_successful_runs(runs_list_df, reset_index=True)

# get a df of [path, run_uuid], where we filter new_paths, only including the paths with run_uuids that were successful.
runs_to_gather_df = get_runs_to_gather_df(new_paths, successful_runs_list_df)
final_paths_list = runs_to_gather_df['path'].to_list()

print("getting df from paths\n")
# get the actual runs from the successful runs paths
all_runs_df = get_df_from_paths(final_paths_list, batch_size=50)
# all_runs_df = pd.read_csv(phase1_files['runs_df'], index_col=0)

print("getting finalized dataframe")
# remove rows with na values for run_start, run_end, renaming all others to  start or stop
merged_df = remove_na_rows(merged_df)
merged_df = merged_df.reset_index(drop=True)

# save final_df
print(merged_df)
merged_df.to_csv(phase1_files['write'])

# update old_paths txt to include newly found paths
append_txt_file(new_paths)
# clear files_not_found txt
'''