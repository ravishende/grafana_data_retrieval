import pandas as pd
import shutil

'''
file to get rid of all unnecessary columns for training a model
and dressing up anything else to help
'''

# settings
read_file = 'csv_files/all_metrics_ratios_added.csv'
write_file = 'csv_files/all_metrics_training_data.csv'

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

'''
=================================
        Columns Creation
=================================
'''
# queried metrics that have been used to create ratio columns
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

# columns to drop
useless_columns = [
    'path',
    'start',
    'stop', 
    'timestep',  # no variations in values - all 600
    'ensemble_uuid', 
    # 'run_uuid', 
]

# get the numerator column of each ratio column. 
# It will be dropped since it isn't useful with the newly added ratio columns.
numerator_columns_t1 = [name + "_t1" for name in metrics]
numerator_columns_t2 = [name + "_t2" for name in metrics]
numerator_columns = numerator_columns_t1 + numerator_columns_t2

# with the ratio columns added, the numerators of the ratio columns no longer become useful
useless_columns = useless_columns + numerator_columns


'''
=============================
        Main Program
=============================
'''

# read csv and drop unnecessary columns
whole_df = pd.read_csv(read_file, index_col=0)
training_data = whole_df.drop(columns=useless_columns)

# write updated df to csv file and print it
training_data.to_csv(write_file)
print("\n"*3)
print(training_data, "\n"*3)