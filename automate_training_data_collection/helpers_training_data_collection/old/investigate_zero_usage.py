import pandas as pd
import shutil
from termcolor import colored
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
grandparent = os.path.dirname(parent)
sys.path.append(grandparent)
from helpers.printing import print_title

'''
This file is to look at the summed runs and figure out statistics on runs
that have nonzero values for cpu and mem total versus those that don't.
For more information on distribution, you can also run data_analysis.py
'''

# settings
read_file = "csv_files/summed.csv"
zeros_write_file = "csv_files/zeros.csv"
non_zeros_write_file = "csv_files/non_zeros.csv"

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# get runs that have values instead of json for queried metrics
summed = pd.read_csv(read_file, index_col=0)

# split summed into runs by runs where cpu_total or mem_total are 0 versus where they both have values
zero_mask = (summed['cpu_total'] == 0) | (summed['mem_total'] == 0)
zeros = summed[zero_mask]
zeros = zeros.reset_index(drop=True)
non_zeros = summed[~zero_mask]
non_zeros = non_zeros.reset_index(drop=True)

# some runs likely have no data for cpu and mem total because the run was too short
# to get kubernetes data. nontrivial_zeros does not include those runs
nontrivial_zeros = zeros[zeros['runtime'] > 45]
nontrivial_zeros = nontrivial_zeros.reset_index(drop=True)

# get statistics on lowest and highest runtime in nontrivial_zeros for displaying
lowest_runtime = nontrivial_zeros['runtime'].min()
highest_runtime = nontrivial_zeros['runtime'].max()

# print dataframes and statistics
print_title("Non Zeros DataFrame")
print(non_zeros)
print_title("Zeros DataFrame")
print(zeros, "\n"*10)
print_title("Nontrivial Zeros DataFrame")
print(nontrivial_zeros, "\n\nLowest runtime:", lowest_runtime, "seconds\nHighest runtime:", round(highest_runtime/3600, 1), "hours", "\n"*10)

# save dataframes to csv files
zeros.to_csv(zeros_write_file)
non_zeros.to_csv(non_zeros_write_file)