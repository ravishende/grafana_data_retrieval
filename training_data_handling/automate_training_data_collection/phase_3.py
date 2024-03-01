import pandas as pd
import shutil
from itertools import chain
from workflow_files import PHASE_3_FILES
import sys
import os
sys.path.append("../../grafana_data_retrieval")  # Adjust the path to go up two levels
parent = os.path.dirname(os.path.realpath(__file__))
grandparent = os.path.dirname(parent)
sys.path.append(grandparent)
from query_resources import query_and_insert_columns

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


'''
==========================
    Helper functions
==========================
'''
class Phase_1():
    def __init__(self, files=PHASE_3_FILES):
        self.files = files
        # metrics (to be queried)
        self.all_metrics = [
            "cpu_usage",
            "mem_usage",
            "cpu_request",
            "mem_request",
            "transmitted_packets",
            "received_packets",
            "transmitted_bandwidth",
            "received_bandwidth"
            ]
        self.static_metrics = ["cpu_request", "mem_request"]
        self.non_static_metrics = [metric for metric in self.all_metrics if metric not in self.static_metrics]
        # duration columns (for querying)
            # Note: assumes duration columns are in the form "duration_t{N}" where {N} is 
            # an int from 1 to num_duration_cols inclusive
        self.num_duration_cols = self._init_num_duration_cols()
        self.duration_col_names = ["duration_t" + str(num) for num in range(1, self.num_duration_cols+1)]
        self.duration_col_total = "runtime"  # from phase_2
        # column names (for queried results)
        self.col_names_static = self.static_metrics
        self.col_names_total = [name + "_total" for name in self.non_static_metrics]
        self.col_names_by_time = self._init_col_names_by_time()
        self.all_col_names = self.col_names_static + self.col_names_total + list(chain.from_iterable(self.col_names_by_time))

    # read a txt file written by phase_2 that contains num_duration_cols
    def _init_num_duration_cols(self):
        # read the file to get num_duration_cols
        try:
            with open(self.files['num_duration_cols'],"r") as f:
                num_duration_cols = int(f.read())
        # handle errors for reading file
        except FileNotFoundError:
            print(f"Error: File {self.files['num_duration_cols']} not found.")
        except ValueError:
            print(f"Error: Content of {self.files['num_duration_cols']} is not an integer.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        # return if successful
        return num_duration_cols

    # initialize col_names_by_time based on self.num_duration_cols and self.non_static_metrics
    def _init_col_names_by_time(self):
        col_names_by_time = []
        for i in range(1, self.num_duration_cols):
            col_names_t_i = [name + "_t" + str(i) for name in self.non_static_metrics]
            col_names_by_time.append(col_names_t_i)
        return col_names_by_time

    # given: 
        # df - a dataframe 
        # batch_size - number of rows to query at a time until the df is filled out
    # query all important metrics, saving to the temporary_save_file after inserting columns of the same duration column.
    # return the updated dataframe with all columns queried
    def query_metrics(self, df, batch_size):
        # initialize all queried column names in the dataframe if they aren't already
        for col_name in self.all_col_names:
            if col_name not in df.columns:
                df[col_name] = None

        # while there are still unqueried rows, keep querying batch_size rows at a time
        while df[self.col_names_total[0]].iloc[len(df)-1] is None:
            # query and insert static and total columns
            df = query_and_insert_columns(df, self.static_metrics, self.col_names_static, self.duration_col_total, batch_size)
            df.to_csv(self.files['temp'])
            df = query_and_insert_columns(df, self.non_static_metrics, self.col_names_total, self.duration_col_total, batch_size)
            df.to_csv(self.files['temp'])
            # query and insert duration_t_i columns
            for i, col_names_t_i in enumerate(self.col_names_by_time):
                df = query_and_insert_columns(df, self.non_static_metrics, col_names_t_i, self.duration_col_names[i], batch_size)
                df.to_csv(self.files['temp'])

        return df

    '''
    ======================
        Main Program
    ======================
    '''
    def run(self):
        # get preprocessed_df
        preprocessed_df = pd.read_csv(phase3_files['read'], nrows=3)

        # 7. query resource metrics (metrics total, t1, t2)
        rows_batch_size = 20
        queried_df = self.query_metrics(preprocessed_df, rows_batch_size, temporary_save_file)

        # save df to a csv file
        queried_df.to_csv(phase3_files['write'])
        print(queried_df)
