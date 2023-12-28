import pandas as pd
pd.set_option('display.max_columns', None)

'''
file to get rid of all unnecessary columns for training a model
and dressing up anything else to help
(like changing cpu_t1 to be an average collected up until t1)
'''

read_file = 'csv_files/summed_success.csv'
write_file = 'csv_files/training_data.csv'


useless_columns = [
    'path',
    'start',
    'stop',
    'timestep',  # maybe? - needs more analysis
    'ensemble_uuid',
    'run_uuid'
]

# read csv and drop unnecessary columns
summed = pd.read_csv(read_file, index_col=0)
summed = summed.drop(columns=useless_columns)

# write updated df to csv file and print it
summed.to_csv(write_file)
print(summed)