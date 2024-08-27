# autopep8: off
import sys
import os
import warnings
import shutil
import ast
import random
from datetime import datetime
import pandas as pd
from workflow_files import PHASE_2_FILES
from metrics_and_columns_setup import GET_DURATION_COLS
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# pylint: disable=wrong-import-position
from helpers.time_functions import datetime_ify
# autopep8: on

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


class Phase_2():
    # files are only read, so pylint: disable=dangerous-default-value
    def __init__(self, files: dict[str, str] = PHASE_2_FILES) -> None:
        self.files = files
        self.num_duration_cols = GET_DURATION_COLS()['num_cols']
        self.drop_cols = [
            "path",
            "extent"
        ]

    # ==========================
    #     Helper functions
    # ==========================

    def _drop_no_extent(self, df: pd.DataFrame, reset_index: bool = True) -> pd.DataFrame:
        df = df.dropna(subset='extent')
        if reset_index:
            df = df.reset_index(drop=True)
        return df

    def _calculate_area(self, corners_list: str) -> float:
        # where p1 in the bottom left = (x1,y1) and p2 in the bottom left = (x2,y2)
        # corners_list is of the form [x1, y1,, x2, y2]
        corners_list = ast.literal_eval(
            corners_list)  # converting string to list
        x1, y1, x2, y2 = float(corners_list[0]), float(
            corners_list[1]), float(corners_list[2]), float(corners_list[3])
        x_length = abs(x2-x1)
        y_length = abs(y2-y1)
        area = x_length * y_length
        return area

    def _calculate_runtime(self, start: str, stop: str) -> float:
        # get start and stop down to the second, no fractional seconds.
        start = start[0:start.find(".")]
        stop = stop[0:stop.find(".")]

        # get start and stop as datetimes
        parsing_successful = False
        # there are two slightly different ways that time strings are represented.
        # Try the other if the first doesn't work.
        format_strings = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]
        for format_str in format_strings:
            try:
                start_dt = datetime.strptime(start, format_str)
                stop_dt = datetime.strptime(stop, format_str)
                # if parsing is successful, break the loop
                parsing_successful = True
                break
            except ValueError:
                continue  # if parsing failed, try next format string
        if not parsing_successful:
            raise ValueError("Time format not recognized")

        # find the difference between stop and start for the runtime
        runtime_delta = stop_dt - start_dt
        return runtime_delta.total_seconds()

    def add_queue_seconds_if_applicable(self, df: pd.DataFrame) -> pd.DataFrame:
        # if there's no queue time given, don't change it
        if not ('queue_time' in df.columns):
            return df
        df['queue_time'] = df['queue_time'].apply(datetime_ify)
        df['start'] = df['start'].apply(datetime_ify)
        # get a new column of the start - queue time to get total seconds
        df['queue_seconds'] = (
            df['queue_time'] - df['start']).dt.total_seconds()
        df['queue_seconds'] = df['queue_seconds'].apply(abs)
        df = df.drop(columns=['queue_time'])
        return df

    def rename_start_stop(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {
            'run_start': 'start',
            'run_end': 'stop'
        }
        df = df.rename(columns=rename_dict)
        return df

    # given a dataframe with 'extent', 'start', and 'stop' columns,
    # return a df with added 'area' and 'runtime' columns
    def add_area_and_runtime(self, df: pd.DataFrame) -> pd.DataFrame:
        # extent is required to calculate area - drop rows that don't contain it
        df = self._drop_no_extent(df, reset_index=True)
        # calculate area and runtime
        df['area'] = df['extent'].apply(self._calculate_area)
        df['runtime'] = df.apply(lambda row: self._calculate_runtime(
            row['start'], row['stop']), axis=1)
        return df

    def drop_columns(self, df: pd.DataFrame, columns_to_drop: list[str], reset_index: bool = True) -> pd.DataFrame:
        df = df.drop(columns=columns_to_drop)
        if reset_index:
            df = df.reset_index(drop=True)
        return df

    # generate random values between run_start and some end time, put into duration1
    def _insert_rand_refresh_col(self, df: pd.DataFrame, refresh_title: str, method: int = 0) -> pd.DataFrame:
        duration_seconds = df['runtime']
        if method == 0:
            # generate random values between 45sec and 5min
            df[refresh_title] = duration_seconds.apply(
                lambda time: random.randint(45, 300) if time >= 300 else time)
        elif method == 1:
            # generate random values between 45sec and half of the duration
            df[refresh_title] = duration_seconds.apply(
                lambda time: random.randint(45, time // 2) if time // 2 >= 45 else time)
        elif method == 2:
            # generate random values between 45sec and the full duration
            df[refresh_title] = duration_seconds.apply(
                lambda time: random.randint(45, time) if time > 45 else time)
        else:
            raise ValueError("method must be: 0, 1, or 2")
        return df

    # given a dataframe and number of duration columns to insert, (also single_method, which is either -1 (not a single method) or some int between 0 and 2)
    # return an updated dataframe with an added n duration columns of various insert methods
    def insert_n_duration_columns(self, df: pd.DataFrame, n: int, single_method: int = -1) -> pd.DataFrame:
        num_insert_methods = 3
        # warn the user if they are expecting more insert methods than are available in _insert_rand_refresh_col
        if n > num_insert_methods and not single_method:
            warnings.warn(
                "There are more columns requested than insert methods defined. Repeating the last method after other methods are used.")
        for i in range(0, n):
            # get the insert method
            if single_method != -1:
                insert_method = single_method
            else:
                insert_method = i
                if insert_method >= num_insert_methods:
                    insert_method = num_insert_methods - 1
            # assemble the duration_title
            duration_title = "duration_t" + str(i + 1)
            # insert the duration column into the df
            df = self._insert_rand_refresh_col(
                df, duration_title, method=insert_method)
        return df

    # ======================
    #     Main Program
    # ======================

    # runs the whole phase. Returns True if successful, False otherwise
    def run(self) -> bool:
        success = False
        # get df from csv file
        ids_included_df = pd.read_csv(self.files['read'], index_col=0)
        # rename 'run_start', 'run_end' columns to 'start', 'stop' for add_area_and_runtime method to work (as well as many other methods in different phases)
        ids_included_df = self.rename_start_stop(ids_included_df)
        # calculate area and runtime and add those columns to dataframe
        calculated_df = self.add_area_and_runtime(ids_included_df)
        calculated_df = self.add_queue_seconds_if_applicable(calculated_df)
        # drop self.drop_cols (columns that were used to calculate area)
        filtered_df = self.drop_columns(
            calculated_df, self.drop_cols, reset_index=True)
        # add duration_t1, duration_t2, etc. columns
        # self.num_duration_cols is the number of duration columns to insert and query for (doesn't include "runtime")
        preprocessed_df = self.insert_n_duration_columns(
            filtered_df, self.num_duration_cols)
        # save preprocessed_df to file and print it
        preprocessed_df.to_csv(self.files['write'])
        print(preprocessed_df)

        success = True
        return success
