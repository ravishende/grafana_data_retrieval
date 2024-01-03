import pandas as pd
import shutil
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("query_resources.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.printing import print_title


# settings and constants
read_file = "csv_files/non_zeros.csv"
write_file = "csv_files/ratios_added.csv"

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# given a dataframe, name of a new column to insert, numerator column, and denominator column
# return the updated dataframe with a new column inserted as a ratio of numerator_col/denominator_col
def insert_ratio_col(df, col_to_insert, numerator_col, denominator_col):
    df[col_to_insert] = df[numerator_col] / df[denominator_col]
    return df


# get dataframe from csv file
training_data = pd.read_csv(read_file, index_col=0)

# print the original dataframe before inserting ratio columns
print_title("Original DataFrame")
print(training_data, "\n"*5)

# insert ratio columns
training_data = insert_ratio_col(training_data, "cpu_t1_ratio", "cpu_t1", "duration_t1")
training_data = insert_ratio_col(training_data, "mem_t1_ratio", "mem_t1", "duration_t1")
training_data = insert_ratio_col(training_data, "cpu_t2_ratio", "cpu_t2", "duration_t2")
training_data = insert_ratio_col(training_data, "mem_t2_ratio", "mem_t2", "duration_t2")

# training_data.to_csv(write_file)
print_title("DataFrame with Columns Inserted")
print(training_data)