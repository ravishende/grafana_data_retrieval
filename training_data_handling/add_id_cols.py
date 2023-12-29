import pandas as pd
import shutil

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

'''
This file should not need to be run anymore.
It is just for adding ensemble uuid and run uuid columns to the queried runs.
It could be done earlier, but all previous py files to this would have to be rerun on those updated csv files.
'''


queried_csv = 'csv_files/queried.csv'
successful_runs_csv = 'csv_files/successful_bp3d_runs.csv'
write_file = 'csv_files/queried_w_ids.csv'

# get csv files as dataframes
successful_runs = pd.read_csv(successful_runs_csv, index_col=0)
queried_runs = pd.read_csv(queried_csv, index_col=0)

# choose columns of successful_runs to add to queried_runs, then merge those columns to queried_runs in a new df
successful_runs_subset = successful_runs[['ensemble_uuid', 'run_uuid']]
merged_df = pd.merge(queried_runs, successful_runs_subset, left_index=True, right_index=True)

# write merged df to a file and print it
print(merged_df)
merged_df.to_csv(write_file)