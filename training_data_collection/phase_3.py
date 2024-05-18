from distutils.command.build_scripts import first_line_re
import math
import shutil
import pandas as pd
from termcolor import colored
from workflow_files import PHASE_3_FILES
from metrics_and_columns_setup import METRICS, DURATION_COLS, COL_NAMES
from helpers_training_data_collection.query_resources import query_and_insert_columns, set_verbose

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

class Phase_3():
    def __init__(self, files=PHASE_3_FILES, metrics=METRICS, duration_cols=DURATION_COLS, col_names=COL_NAMES):
        self.files = files
        # metrics (to be queried)
        self.all_metrics = metrics["all"]
        self.static_metrics = metrics["static"]
        self.non_static_metrics = metrics["non_static"]
        # duration columns (for querying)
        self.num_duration_cols = duration_cols["num_cols"]
        self.duration_col_names = duration_cols["col_names"]
        self.duration_col_total = duration_cols["total_col"]
        # column names (for queried results)
        self.col_names_static = col_names["static"]
        self.col_names_total = col_names["totals"]
        self.col_names_by_time = col_names["by_time"]
        self.all_col_names = col_names["all"]

    # given a pandas series, return the first row in the series that hasn't been queried
    def _get_first_unqueried_row_idx(self, series):
        # get the series as booleans of if it doesn't hold a list or str
        # it will be a str if it was read in from a csv and a list once it's put into the df
        # it will be neither if it is NA, None, or NaN, which is unqueried
        is_unqueried_series = series.apply(lambda row: not isinstance(row, (str, list)))
        # get the first instance where a cell is unqueried
        first_non_queried_row = is_unqueried_series.idxmax()
        return first_non_queried_row

    # given: 
        # df - a dataframe 
        # batch_size - number of rows to query at a time until the df is filled out
            # to save progress after each row, set batch_size = 1
    # query all important metrics, saving to the temporary_save_file after inserting columns of the same duration column.
    # return the updated dataframe with all columns queried
    def query_metrics(self, df, batch_size):
        # try getting saved query progress df
        try:
            # if the first cell of the last queried column in progress_df is not NA, use progress_df instead of df
            progress_df = pd.read_csv(self.files['query_progress'], index_col=0)
            progress_df_final_col = progress_df[self.col_names_total[-1]]
            first_cell = progress_df_final_col.iloc[0]
            if not pd.isna(first_cell):
                df = progress_df
        except:
            pass

        # initialize all queried column names in the dataframe if they aren't already
        for col_name in self.all_col_names:
            if col_name not in df.columns:
                df[col_name] = None

        # set up batches
        last_col_to_query = df[self.col_names_total[-1]]
        num_queried_rows = self._get_first_unqueried_row_idx(last_col_to_query)
        num_rows_to_query = (len(df) - num_queried_rows)
        print(f"{num_queried_rows} rows queried, {num_rows_to_query} left.")
        print(f"Batch size: {batch_size} rows")
        batch_number = 1
        total_batches = math.ceil(num_rows_to_query/batch_size)
        # while there are still unqueried rows, keep querying batch_size rows at a time 
        # loop condition checks if last cell has been queried yet - once queried, it will be a list (empty or not) or str (of a list) if it has been been read in from a csv
        while not isinstance(df[self.col_names_total[-1]].iloc[len(df) -1], (list, str)):
            print(f"\nbatch {batch_number} / {total_batches}:")
            # query and insert static columns
            df = query_and_insert_columns(df, self.static_metrics, self.col_names_static, self.duration_col_total, batch_size)
            # query and insert duration_t_i columns
            # query based on duration columns - do one duration col at a time
            if self.num_duration_cols > 0:
                for i in range(1, self.num_duration_cols+1):
                    # find all columns for this duration_t_i
                    col_names_t_i = [name for name in self.col_names_by_time if name[-1] == str(i)]
                    # query for this duration_t_i column
                    df = query_and_insert_columns(df, self.non_static_metrics, col_names_t_i, self.duration_col_names[i-1], batch_size)  
                    # ^^ duration_col_names is a list that starts at index 0, but duration columns are named duration_t1 through duration_tn, so we loop from i = 1...n, but index at i-1
            # query and insert total columns
            df = query_and_insert_columns(df, self.non_static_metrics, self.col_names_total, self.duration_col_total, batch_size)
            # save batch of partial querying progress
            batch_number += 1
            df.to_csv(self.files['query_progress'])

        return df

    '''
    ======================
        Main Program
    ======================
    '''
    # runs the whole phase. Returns True if successful, False otherwise
    # Note: number of queries = rows_batch_size * 15, so it is better to choose a small number (e.g. 10) for more frequent saving
    def run(self, rows_batch_size=10, verbose_status=False):
        success = False

        # set printing status for query functions later on
        set_verbose(verbose_status)

        # get preprocessed_df
        preprocessed_df = pd.read_csv(self.files['read'], index_col=0)

        # 7. query resource metrics (metrics total, t1, t2)
        queried_df = self.query_metrics(preprocessed_df, rows_batch_size)

        # save df to a csv file
        queried_df.to_csv(self.files['write'])
        print(queried_df)

        success = True
        return success