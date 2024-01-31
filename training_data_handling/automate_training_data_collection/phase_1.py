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
from workflow_files import save


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
print("successfully authenticated")


def get_paths(rangestart,rangeend):
    global fs, bucket
    paths = fs.ls(bucket)
    for path in paths[rangestart:rangeend]:
        paths = fs.ls(path)
        for path in paths:
            get_child_directories(path)

def get_child_directories(path):
    global fs
    global simulation_paths
    paths = fs.ls(path)
    for p in paths:
        if "run_" in p:
            simulation_paths.append(p)

def read_paths(df):
    # csv_file = "csv_files/successful_bp3d_runs.csv"
    # df = pd.read_csv(csv_file, index_col=0)
    paths_df = df["s3_path"]
    paths_list = paths_df.tolist()
    return paths_list

simulation_paths = read_paths()
print("simulation paths length:", len(simulation_paths))

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

def get_df_chunk(stop):
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

pd.set_option('display.max_columns', None)
print(filenotfound)

simulation_paths = read_paths()
df = get_df_chunk(1889)



# remove NaN rows
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


if len(df > 0):
    df.to_csv('csv_files/unfiltered.csv', mode='a')


'''