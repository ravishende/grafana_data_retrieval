import pandas as pd
from termcolor import colored
import ast
from pprint import pprint
from datetime import datetime
pd.set_option('display.max_columns', None)

'''
file for taking in a csv of raw bp3d runs data and dropping unneccessary 
information as well as calculating area and runtime from the data.
'''

un_filtered_df = pd.read_csv("training_data_initial.csv")

useful_columns = [
    "start", 
    "stop", 
    "sim_time", 
    "timestep", 
    "wind_direction",
    "wind_speed", 
    "canopy_moisture", 
    "surface_moisture", 
    "extent",  
    "run_max_mem_rss_bytes",
     "threads"
]

filtered_df = un_filtered_df.get(useful_columns)

# on several rows, there are faulty datapoints displaying the column
# names as their values. This causes an error if these rows aren't dropped
drop_condition = filtered_df['stop'] == 'stop'
filtered_df = filtered_df.drop(filtered_df[drop_condition].index)
filtered_df.dropna(subset=['extent', "start", "stop"], inplace=True)
filtered_df = filtered_df.reset_index()

def calculate_area(L):
    #where p1 in the bottom left = (x1,y1) and p2 in the bottom left = (x2,y2)
    # Converting string to list
    res = ast.literal_eval(L)
    L = res
    x1, y1, x2, y2 = float(L[0]), float(L[1]), float(L[2]), float(L[3])
    x_length = abs(x2-x1)
    y_length = abs(y2-y1)
    area = x_length * y_length
    return area

def calculate_runtime(start, stop):
    # there are two slightly different ways that time strings are represented. Try the other if the first doesn't work.
    try:
        # get start and stop down to the second, no fractional seconds.
        start = start[0:start.find(".")]
        stop = stop[0:stop.find(".")]
        # get start and stop as datetimes, then find the difference between them for the runtime
        format_string = "%Y-%m-%d %H:%M:%S"
        runtime_delta = datetime.strptime(stop, format_string) - datetime.strptime(start, format_string)
        return runtime_delta.total_seconds()
    except ValueError:
        # get start and stop as datetimes, then find the difference between them for the runtime
        format_string = "%Y-%m-%dT%H:%M:%S"
        runtime_delta = datetime.strptime(stop, format_string) - datetime.strptime(start, format_string)
        return runtime_delta.total_seconds()


# calculate area, runtime
filtered_df['area'] = filtered_df.apply(lambda row: calculate_area(row['extent']), axis=1)
filtered_df['runtime'] = filtered_df.apply(lambda row: calculate_runtime(row['start'], row['stop']), axis=1)

# after extent is used to calculate area, it is no longer needed. Also drop duplicate index column
filtered_df = filtered_df.drop(columns=['extent', 'index'])

# save filtered data to a new csv file and print the final df
filtered_df.to_csv("cleaned_training_data.csv")
print(filtered_df)
print(colored('success', 'green'))

