import s3fs
import zarr
import json
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import pickle
from pprint import pprint
import os
from dotenv import load_dotenv
from workflow_files import phase1_files
from termcolor import colored


'''
=========================
Phase 1:  Collecting Runs
=========================
1. get successful bp3d runs from bp3d-runs
    - run_selection.py
2. collect runs from successful bp3d runs  # needs new phase?
    - gather.ipynb in collect_runs/

FINISH:
save df to a file
'''




if not load_dotenv():
    raise EnvironmentError("Failed to load the .env file. This file should contain the ACCESS_KEY and SECRET_KEY")

endpoint = 'https://wifire-data.sdsc.edu:9000'
access_key = os.getenv("ACCESS_KEY")
secret_key = os.getenv("SECRET_KEY")

# get fs and bucket
fs = s3fs.S3FileSystem(key=access_key,
    secret=secret_key,
    client_kwargs={
        'endpoint_url': endpoint,
        'verify': False
    },
    skip_instance_cache=False
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   

name = 'quicfire.zarr'
bucket = 'burnpro3d/d'

root = list(fs.ls(bucket))

simulation_paths = []
print(colored("\n\nsuccessfully authenticated", "green"))


def get_child_directories(fs, path):
    sim_paths = []
    paths = fs.ls(path)
    for path in paths:
        if "run_" in path:
            sim_paths.append(path)
    return sim_paths


# Given paths_batch (a list of paths to append to the file),
# appends a batch of paths to 'paths.txt'. Each path is written on a new line.
# If 'paths.txt' does not exist, it is created.
def append_paths_txt(paths_batch):
    with open("paths.txt", "a") as file:  # Open the file in append mode ('a')
        for path in paths_batch:
            file.write(path + "\n")  # Write each path on a new line


def read_paths():
    paths = []
    with open("paths.txt","r") as f:
        paths = f.read().splitlines() 
    return paths


# given a start and end index as well as the file system and bucket, 
# return all the paths in that index range.
def get_paths_batch(start_index, end_index, fs, bucket):
    paths_batch = []
    paths = fs.ls(bucket)
    for path in tqdm(paths[start_index:end_index]):
        paths = fs.ls(path)
        for path in paths:
            paths_batch += get_child_directories(fs, path)
    return paths_batch

# batch size is a number of paths per batch to get
def get_all_paths(fs, bucket, batch_size=None):
    paths_len = len(fs.ls(bucket))
    
    # if we're not using batches, run everything at once
    if batch_size is None:
        sim_paths = get_paths_batch(0, paths_len, fs, bucket)
        append_paths_txt(sim_paths)
        return sim_paths

    # intialize a list to hold all simulation paths
    try:
        all_simulation_paths = read_paths()
    except FileNotFoundError:    
        all_simulation_paths = []

    # collect runs in batches
    num_batches = paths_len//batch_size
    for i in range(0, num_batches):
        # define indices to get paths batch for
        start_index = i*batch_size
        end_index = (i+1)*batch_size

        num_gathered_paths = len(all_simulation_paths)
        
        # don't re-gather already found paths
        if end_index > num_gathered_paths:
            continue
        # update start index if it's less than what has been gathered
        if start_index < num_gathered_paths:
            start_index = num_gathered_paths

        # make sure to get all of the paths on the final batch
        if end_index == num_batches-1:
            end_index = paths_len

        # get simulation paths for this batch
        print(f"\nGetting batch for indices {start_index} up to {end_index}.", colored(f"Batch {i+1}/{num_batches}", "blue"))
        sim_paths_batch = get_paths_batch(start_index, end_index, fs, bucket)
        all_simulation_paths += sim_paths_batch

        # append newly collected paths to the paths.txt file
        append_paths_txt(sim_paths_batch)

    return all_simulation_paths


KEEP_ATTRIBUTES = {
    'path': lambda d: None,
    'time_scraped': lambda d: None,
    'canopy_moisture': lambda d: d['canopy_moisture'],
    'dz':lambda d: d['dz'],
    'extent': lambda d: d['extent'],
    'extent_fmt': lambda d: d['extent_fmt'],
    'fire_grid': lambda d: d['fire_grid'],
    'fuel': lambda d: d['fuel'],
    'ignition': lambda d: d['ignition'],
    'output': lambda d: d['output'],
    'resolution': lambda d: d['resolution'],
    'resolution_units': lambda d: d['resolution_units'],
    'run_binary': lambda d: d['run_binary'],
    'run_end': lambda d: d['run_end'],
    'run_max_mem_rss_bytes': lambda d: d['run_max_mem_rss_bytes'],
    'run_start': lambda d: d['run_start'],
    'seed': lambda d: d['seed'],
    'sim_time': lambda d: d['sim_time'],
    'surface_moisture': lambda d: d['surface_moisture'],
    'threads': lambda d: d['threads'],
    'timestep': lambda d: d['timestep'],
    'topo': lambda d: d['topo'],
    'wind_direction': lambda d: d['wind_direction'],
    'wind_speed': lambda d: d['wind_speed']
}


def get_df_chunk(start, stop, paths, files_not_found):
    # initialize a list of paths that cause filenotfound errors
    filenotfound = []
    
    # initialize dataframe
    global KEEP_ATTRIBUTES
    df = pd.DataFrame([], columns=KEEP_ATTRIBUTES.keys())
    time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    
    # don't try to access out of bounds of path
    if stop > len(paths):
        stop = len(paths)

    print(f"Reading from line {start} to {stop}")

    row_index = 0
    for path in tqdm(paths[start:stop]):
        # try to get the file from the path
        try:
            with fs.open(path + '/' + name + '/.zattrs') as file:
                data=json.load(file)
        # if the file isn't there, append path to filenotfound
        except:
            filenotfound.append(path)
            continue
        

        row = []
        for attribute in KEEP_ATTRIBUTES.keys():
            try:
                value = KEEP_ATTRIBUTES[attribute](data)
            except KeyError:
                value = None
            row.append(value)

        row[0] = path
        row[1] = time
        df.loc[row_index] = row
        row_index+=1

    # update vars.txt with the next start
    with open("vars.txt", "w") as f:
        f.write(str(stop))

    # print file not found files
    print("FileNotFound Error on the following Files:")
    for file_path in filenotfound:
        print("\t" + file_path)
    print(colored(f"\nRead from {start} to {stop}\n", "green"))

    # return df and files not found
    files_not_found += filenotfound
    return df, files_not_found



# given the simulation paths, create a df containing runs
# for all paths that have corresponding files
def get_df_from_paths(simulation_paths, batch_size=1000):
    # calculate how many batches to run
    num_batches = len(simulation_paths) // batch_size
    files_not_found = []
    
    # find out how many runs are previously collected
    try:
        runs_df = pd.read_csv(phase1_files['temp'], index_col=0)
        num_gathered_runs = len(runs_df)
    except FileNotFoundError:
        num_collected_runs = 0


    # get df_chunk and append it to a csv file for each batch
    for batch_i in range(1, num_batches):
        print(colored(f"batch {batch_i}/{num_batches}:", "green"))

        # get the index to start and stop at in get_df_chunk
        stop_index = batch_i*batch_size
        start_index = stop_index-batch_size

        # if this is the last iteration, get the df for the rest of the paths
        if batch_i == num_batches:
            stop_index = len(simulation_paths)
        
        # don't regather already collected runs
        if stop_index < num_gathered_runs:
            print("\truns already gathered - skipping batch")
            continue
        # update start_index if it's less than what's been gathered
        if start_index < num_gathered_runs:
            start_index = num_gathered_runs

        # get the df from the runs
        partial_runs_df, files_not_found = get_df_chunk(start_index, stop_index, simulation_paths, files_not_found)

        # save the df to a file
        if len(partial_runs_df) > 0:
            partial_runs_df.to_csv(phase1_files['temp'], mode='a')
            print(partial_runs_df)

    # get the total runs df and return it
    runs_df = pd.read_csv(phase1_files['temp'], index_col=0)
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


# given a df returns an updated df with the NaN time rows removed
def remove_na_rows(df):
    # crucial columns that need to have data. If they do not, that means the run failed somewhere
    time_cols = ['run_start', 'run_end', 'sim_time']

    # Create a new DataFrame that includes rows with NA values in 'start', 'stop', or 'runtime'
    # Then store it in a csv file called na_times
    na_mask = df[time_cols].isna()
    na_rows_df = df[na_mask.any(axis=1)]
    if len(na_rows_df > 0):
        na_rows_df.to_csv('csv_files/na_times.csv', mode='a')

    # Drop columns with NA values in any of the time columns
    df = df.dropna(subset=time_cols)

    # Rename the 'run_end' column to stop and run_start column to 'start'
    df = df.rename(columns={"run_end": "stop", "run_start": "start"})
    df = df.drop(columns=["ignition","fuel"], axis=1)
    return df



'''
======================
    Main Program
======================
'''

pd.set_option('display.max_columns', None)

# get a df that only contains the ids and status of successful runs
runs_list_df = pd.read_csv(phase1_files['read'])
successful_runs_list_df = get_successful_runs(runs_list_df, reset_index=True)

# get the paths of the successful runs
simulation_paths = read_paths()
print("simulation paths length:", len(simulation_paths))

# get the actual runs from the successful runs paths
runs_df = get_df_from_paths(simulation_paths, batch_size=1000)


