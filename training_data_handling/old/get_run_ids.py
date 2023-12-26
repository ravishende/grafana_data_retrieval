import pandas as pd
from termcolor import colored
import datetime as datetime


'''
Note: 
This file was just to get run_ids for the newer performance training data.
This is done now, so this file should not need to be revisited.
'''

# settings
filtered_csv = "csv_files/" #performance_training_data.csv
unfiltered_csv = "csv_files/" #training_data_initial.csv
training_data_csv = "csv_files/" #p2_training_data.csv
write_df_csv = "csv_files/" #p2_run_ids_included.csv
write_runs_series_csv = "csv_files/" #run_ids.csv
pd.set_option('display.max_columns', None)

# collect run_id from path
def get_run_id(path):
    if path is None: 
        return path

    run_id = path.split("/")[-1]
    return run_id

# collect dataframes
filtered_df = pd.read_csv(filtered_csv, index_col=0)
original_df = pd.read_csv(unfiltered_csv, index_col=0)
training_data = pd.read_csv(training_data_csv, index_col=0)

# get paths from original_df and put in filtered_df
merged_df = pd.merge(
        filtered_df, 
        original_df[['start', 'stop', 'path']], 
        on=['start', 'stop'], 
        how='left'
    )



# get run_id for df, drop path
training_data['run_id'] = merged_df['path'].apply(get_run_id)

# write to file
training_data.to_csv(write_df_csv)

# # save just run_ids
# run_ids = merged_df['run_id']
# run_ids.to_csv(write_runs_series_csv)



# print_special("run_id_df")
# print(run_id_df)
# print("*"*50, len(run_id_df))

# print_special("Original DF")
# print(original_df)

# print_special("Filtered DF")
# print(filtered_df)

print(training_data)