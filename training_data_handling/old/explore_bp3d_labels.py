import pandas as pd 
from ast import literal_eval
# settings
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
read_file = "csv_files" #worker_templates.csv

df = pd.read_csv(read_file, index_col=0)
templates = df["id"].apply(literal_eval)