import pandas as pd
import shutil

'''
file to get rid of all unnecessary columns for training a model
and dressing up anything else to help
(like changing cpu_t1 to be an average collected up until t1)
'''

# settings
read_file = 'csv_files/ratios_added.csv'
write_file = 'csv_files/training_data.csv'

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


useless_columns = [
    'path',
    'start',
    'stop', 
    'timestep',  # no variations in values - all 600
    'ensemble_uuid', 
    'run_uuid' 
]

# read csv and drop unnecessary columns
whole_df = pd.read_csv(read_file, index_col=0)
training_data = whole_df.drop(columns=useless_columns)

# write updated df to csv file and print it
training_data.to_csv(write_file)
print("\n"*3)
print(training_data, "\n"*3)