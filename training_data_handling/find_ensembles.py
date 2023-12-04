import pandas as pd
# from resource_json_summation import 

'''
NOTE: 
This file is for adding a new column to training data
called 'ensemble' that contains ensemble_ids for each
run in the training data. If there is not a known 
ensemble id for a run in training_data, its value in
the 'ensemble' column will be None
'''

# settings and constants
ids_csv = "csv_files/" #run_ensemble_ids.csv
training_csv = "csv_files/" #p2_run_ids_included.csv
write_csv = "csv_files/" #p2_ensemble_included.csv
pd.set_option("display.max_columns", None)


# Use: 
#  Given two dataframes and an ensemble column title, insert a new ensemble column into df_to_update
#  with ensemble_ids corresponding at the matching run_ids in conversion_table
# Parameters:
#  - df_to_update: training data df. must contain column 'run_id'
#  - conversion_table: df containing two columns: 'run_uuid' and 'ensemble_uuid'. run_uuids all exist in df_to_update['run_id']
#  - ensemble_title: title of the ensemble column to insert into the new df. e.g. "ensemble_id"
# Returns: 
#  a new data frame that is the same as df_to_update plus a new column 'ensemble'.
def add_ensembles(df_to_update, conversion_table, ensemble_title):
    # merge conversion_table with df_to_update at run_ids to add in an ensemble_id column
    merged_df = pd.merge(df_to_update, conversion_table, left_on='run_id', right_on='run_uuid', how='left')

    # Renaming columns and dropping extra column
    merged_df = merged_df.rename(columns={'ensemble_uuid': ensemble_title})
    final_df = merged_df.drop(columns=['run_uuid'])
    return final_df


def change_run_id_format(run):
    return "run_" + run

# read csv files
conversion_table = pd.read_csv(ids_csv, index_col=0)
training_data = pd.read_csv(training_csv, index_col=0)

conversion_table['run_uuid'] = conversion_table['run_uuid'].apply(change_run_id_format)
# add ensemble title
training_data = add_ensembles(training_data, conversion_table, "ensemble")

# save updated training data to a new csv file
training_data.to_csv(write_csv)

# find out how many ensembles were there
na_ensemble_count = training_data['ensemble'].isna().sum()
non_na_ensemble_count = len(training_data) - na_ensemble_count

print(training_data.head())
print("")
print(na_ensemble_count, "ensembles are NA")
print(non_na_ensemble_count, "ensembles have data")

# print(training_data)