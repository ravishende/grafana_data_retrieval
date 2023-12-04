import pandas as pd
pd.set_option('display.max_columns', None)

'''
This file is just to see how different csv files of dataframes look.
It does not edit them or do anything other than print.
'''

def print_df(df_title, df):
    print("="*50, df_title, "="*50, sep="\n")
    print(df, "\n"*5)

runs = pd.read_csv("csv_files/bp3d-runs.csv", index_col=0)
successful_runs = pd.read_csv("csv_files/successful_bp3d_runs.csv", index_col=0)
unfiltered = pd.read_csv("csv_files/unfiltered.csv", index_col=0)
filtered = pd.read_csv("csv_files/area_runtime_filtered.csv", index_col=0)

print_df("Runs", runs)
print_df("Successful Runs", successful_runs)
print_df("unfiltered", unfiltered)
print_df("filtered", filtered)