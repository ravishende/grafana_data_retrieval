# autopep8: off
import pandas as pd
import shutil
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.printing import print_title
# autopep8: on

# settings and constants
read_file = "csv_files/summed.csv"
write_file = "csv_files/summed_w_ratios.csv"

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


'''
==============================
        User Functions
==============================
'''


# given a dataframe, name of a new column to insert, numerator column, and denominator column
# return the updated dataframe with a new column inserted as a ratio of numerator_col/denominator_col
def insert_ratio_col(df, col_to_insert, numerator_col, denominator_col):
    df[col_to_insert] = df[numerator_col].astype(
        float) / df[denominator_col].astype(float)
    return df


# given a dataframe, name of a new columns to insert, numerator columns, and duration column (denominator)
# return the updated dataframe with new columns inserted as a ratio of numerator_col/denominator_col
def insert_ratio_columns(df, cols_to_insert, numerator_cols, duration_col):
    for insert_col, numerator_col in zip(cols_to_insert, numerator_cols):
        df[insert_col] = df[numerator_col].astype(
            float) / df[duration_col].astype(float)
    return df


# given a list of numerator columns, return a df without those numerator columns in it
def drop_numerator_columns(df, numerator_cols):
    df = df.drop(columns=numerator_cols)
    return df


'''
=================================
        Columns Creation
=================================
'''
# queried metrics that should be ratio columns
metrics = [
    "cpu_usage",
    "mem_usage",
    "cpu_request_%",
    "mem_request_%",
    "transmitted_packets",
    "received_packets",
    "transmitted_bandwidth",
    "received_bandwidth"
]

t1_metric_columns = [name + "_t1" for name in metrics]
t2_metric_columns = [name + "_t2" for name in metrics]

t1_insert_columns = [name + "_t1_ratio" for name in metrics]
t2_insert_columns = [name + "_t2_ratio" for name in metrics]


'''
============================
        Main Program
============================
'''

# get dataframe from csv file
training_data = pd.read_csv(read_file, index_col=0)

# print the original dataframe before inserting ratio columns
print_title("Original DataFrame")
print(training_data, "\n"*5)

# insert ratio columns
training_data = insert_ratio_columns(
    training_data, t1_insert_columns, t1_metric_columns, "duration_t1")
training_data = insert_ratio_columns(
    training_data, t2_insert_columns, t2_metric_columns, "duration_t2")
# with the ratio columns added, the numerators of the ratio columns no longer become useful
df = drop_numerator_columns(t1_metric_columns)
df = drop_numerator_columns(t2_metric_columns)

# print updated dataframe and save it to a file
print_title("DataFrame with Columns Inserted")
print(training_data)
training_data.to_csv(write_file)
