import pandas as pd
import shutil
from preprocessing import preprocess_df
from querying import Query_handler
from finalizing import Finalizer


# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# given a dataframe with 'start' and 'stop', and potentially other columns,
# query it over those times with given/selected queries
read_file = "csvs/read.csv"

# get the df & make sure there's no unnamed column from if csv was saved with/without an index col
df = pd.read_csv(read_file)
unnamed_cols = df.columns.str.match('Unnamed')
runs_list_df = df.loc[:, ~unnamed_cols]

if not 'start' in df.columns or not 'stop' in df.columns:
    raise ValueError(
        "dataframe must have a 'start' column and a 'stop' columnn")


num_partial_duration_cols = input("How many duration columns should there be?")
try:
    num_partial_duration_cols = int(num_partial_duration_cols)
except:
    raise ValueError("Input must be an int.")

query_handler = Query_handler()
finalizer = Finalizer()

df = preprocess_df(df, num_partial_duration_cols)
df = query_handler.query_df(df)
df = finalizer.sum_df(df)
print("Finalized dataframe:\n", df, sep="")
