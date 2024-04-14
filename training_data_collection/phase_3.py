import pandas as pd
import shutil
from workflow_files import PHASE_3_FILES
from metrics_and_columns_setup import METRICS, DURATION_COLS, COL_NAMES
from helpers_training_data_collection.query_resources import query_and_insert_columns

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

    # given: 
        # df - a dataframe 
        # batch_size - number of rows to query at a time until the df is filled out
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

        # while there are still unqueried rows, keep querying batch_size rows at a time
        while df[self.col_names_total[-1]].iloc[len(df)-1] is None:
            # query and insert static columns
            df = query_and_insert_columns(df, self.static_metrics, self.col_names_static, self.duration_col_total, batch_size)
            df.to_csv(self.files['query_progress'])
            # query and insert duration_t_i columns
            for i, col_names_t_i in enumerate(self.col_names_by_time):
                df = query_and_insert_columns(df, self.non_static_metrics, col_names_t_i, self.duration_col_names[i], batch_size)
                df.to_csv(self.files['query_progress'])
            # query and insert total columns
            df = query_and_insert_columns(df, self.non_static_metrics, self.col_names_total, self.duration_col_total, batch_size)
            df.to_csv(self.files['query_progress'])

        return df

    '''
    ======================
        Main Program
    ======================
    '''
    # runs the whole phase. Returns True if successful, False otherwise
    def run(self, rows_batch_size=20):
        success = False

        # get preprocessed_df
        preprocessed_df = pd.read_csv(self.files['read'], index_col=0)

        # 7. query resource metrics (metrics total, t1, t2)
        queried_df = self.query_metrics(preprocessed_df, rows_batch_size)

        # save df to a csv file
        queried_df.to_csv(self.files['write'])
        print(queried_df)

        success = True
        return success