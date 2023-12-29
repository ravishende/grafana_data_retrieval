import pandas as pd
import shutil

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

'''
This file is for selecting all successful runs from the original csv file that
contains all runs with run_id, ensemble_uuid, run & ens status, and paths for the runs.
You should not need to rerun it. 
This original csv does not contain information on 
input parameters of each run. 
'''

# settings and constants
read_file = "csv_files/bp3d-runs.csv"
write_file = "csv_files/successful_bp3d_runs.csv"

runs = pd.read_csv(read_file)
successful_runs = runs[(runs["ens_status"]=="Done") & (runs["run_status"]=="Done")]
successful_runs = successful_runs.reset_index(drop=True)


successful_runs.to_csv(write_file)
print(successful_runs)