import pandas as pd
import shutil
from query_resources import query_and_insert_columns
from resource_json_summation import 

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

to_query_read = "csv_files/queried.csv"
to_sum_read = "inserted_cpu_r.csv"

# main program
df = pd.read_csv(to_query_read, index_col=0)

# df['cpu_request'] = df['cpu_request'] / df['runtime']
df['cpu_r'] = None
df = query_and_insert_columns(df, ["cpu_request"], ["cpu_r"], "runtime", len(df))
df.to_csv(to_sum_read)


print(df)