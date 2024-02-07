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
print(colored("successfully authenticated", "green"))


def get_child_directories(fs, path):
    sim_paths = []
    paths = fs.ls(path)
    for p in paths:
        if "run_" in p:
            sim_paths.append(p)
    return sim_paths


# Given paths_batch (a list of paths to append to the file),
# appends a batch of paths to 'paths.txt'. Each path is written on a new line.
# If 'paths.txt' does not exist, it is created.
def append_paths_txt(paths_batch):
    with open("paths.txt", "a") as file:  # Open the file in append mode ('a')
        for path in paths_batch:
            file.write(path + "\n")  # Write each path on a new line


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
    if batch_size is None:
        sim_paths = get_paths_batch(0, paths_len, fs, bucket)
        append_paths_txt(sim_paths)
        return sim_paths

    all_simulation_paths = []
    start_index = 0
    end_index = batch_size
    finished_collecting = False
    while not finished_collecting:
        if end_index >= paths_len:
            end_index = paths_len
            finished_collecting = True
        sim_paths_batch = get_paths_batch(start_index, end_index, fs, bucket)
        all_simulation_paths += sim_paths_batch
        append_paths_txt(sim_paths_batch)
        start_index = end_index
        end_index = start_index + batch_size

    return all_simulation_paths



# def read_paths(df):
#     # df = pd.read_csv(read_file, index_col=0)
#     paths_df = df["s3_path"]
#     paths_list = paths_df.tolist()
#     return paths_list

def read_paths():
    paths = []
    with open("paths.txt","r") as f:
        paths = f.read().splitlines() 
    return paths

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
filenotfound = []

def get_df_chunk(stop, paths):
    with open("vars.txt", "r") as file:
        start = int(file.read())
        print("start: line" , start)
    global simulation_paths, KEEP_ATTRIBUTES, incomplete, filenotfound

    df = pd.DataFrame([], columns=KEEP_ATTRIBUTES.keys())
    time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    i = 0
    if stop > len(paths):
        stop = len(paths)
    for p in tqdm(simulation_paths[start:stop]):
        try:
            with fs.open(p + '/' + name + '/.zattrs') as f:
                data=json.load(f)
        except:
            filenotfound.append(p)
            print("FileNotFound error on path {",p,"}")
            continue
            
        row = []
        for k,expr in KEEP_ATTRIBUTES.items():
            try:
                value = KEEP_ATTRIBUTES[k](data)
            except KeyError:
                value = None
            row.append(value)
        row[0] = p
        row[1] = time
        df.loc[i] = row
        i+=1
    with open("vars.txt", "w") as f:
            f.write(str(stop))
            print("\nRead from ", start, " to ", stop,"\n")
    return df

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
print("file_not_found: \n\t", filenotfound)

# get a df that only contains the ids and status of successful runs
runs_list_df = pd.read_csv(phase1_files['read'])
successful_runs_list_df = get_successful_runs(runs_list_df, reset_index=True)

# get the paths of the successful runs
print("\n\n\n\nGetting all paths:\n")
simulation_paths = get_all_paths(fs, bucket)
# simulation_paths = read_paths()
print("simulation paths length:", len(simulation_paths))

# get the actual runs from the successful runs paths
batch_size = 100
num_batches = len(successful_runs_list_df) // batch_size
for i in range(num_batches):
    # if its the last iteration, get the paths until the end of the run
    if i == num_batches-1:
        runs_df = get_df_chunk(len(successful_runs_list_df), simulation_paths)
    else:
        runs_df = get_df_chunk(batch_size*num_batches, simulation_paths)
    # save the df to a file
    if len(runs_df > 0):
        # runs_df.to_csv(phase1_files['write'], mode='a')

         print(runs_df)
