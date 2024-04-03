import pandas as pd
from termcolor import colored
import shutil

# settings
# read_file = "csv_files/summed_all_metrics.csv"
read_file = "csv_files/updated_requests.csv"
non_negative_file = "csv_files/non_neg_updated_requests.csv"
negative_file = "csv_files/neg_updated_requests.csv"

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

# get dataframe
summed_runs = pd.read_csv(read_file, index_col=0)
print(summed_runs, "\n"*5)


# build columns
metrics_l = [
    "cpu_usage",
    "mem_usage",
    "cpu_request",
    "mem_request",
    "transmitted_packets",
    "received_packets",
    "transmitted_bandwidth",
    "received_bandwidth"
    ]
summary_columns = []
for name in metrics_l:
    summary_columns.append(name + "_total")
    summary_columns.append(name + "_t1")
    summary_columns.append(name + "_t2")

negative_mask = (summed_runs['cpu_request']<0) | (summed_runs['mem_request']<0)
negative_indices = summed_runs[negative_mask].index
negative_df = summed_runs.loc[negative_indices]
non_negative_df = summed_runs.drop(index=negative_indices)
non_negative_df = non_negative_df.reset_index(drop=True)
print(colored("Negative Indices", "green"))
print(negative_df, "\n"*5)
print(colored("Non Negative Indices", "green"))
print(non_negative_df)
negative_df.to_csv(negative_file)
non_negative_df.to_csv(non_negative_file)

# make and populate new df
# summary_df = pd.DataFrame(columns=summary_columns)
# for col_name in summary_df.columns:
    # num_negative = summed_runs[negative_mask][col_name].sum()
    # padding = 27 - len(col_name)
    # print(col_name, " "*padding, "\t", colored(num_negative, "green"))

# print df
# print(summary_df)


'''
def print_non_na_indices(df):
    for column in df.columns:
        non_na_indices = df.index[df[column].notna()].tolist()

        # Process for grouping indices
        grouped_indices = []
        start = end = None
        for index in non_na_indices:
            if start is None:
                start = end = index
            elif index == end + 1:
                end = index
            else:
                grouped_indices.append((start, end) if start != end else start)
                start = end = index
        grouped_indices.append((start, end) if start != end else start)

        # Format the output
        formatted_indices = ', '.join([f"[{i[0]},{i[1]}]" if isinstance(i, tuple) else str(i) for i in grouped_indices])
        print(f"Column '{column}': {formatted_indices}")
'''