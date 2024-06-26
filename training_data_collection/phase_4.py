import pandas as pd
from workflow_files import PHASE_4_FILES
from metrics_and_columns_setup import METRICS, DURATION_COLS, COL_NAMES, ID_COLS
from helpers_training_data_collection.resource_json_summation import update_columns, insert_percent_cols

class Phase_4():
    def __init__(self, files=PHASE_4_FILES, metrics=METRICS, duration_cols=DURATION_COLS, col_names=COL_NAMES, id_cols=ID_COLS):
        self.files = files
        self.drop_cols = ["start","stop", "ensemble_uuid"]
        # metrics
        self.static_metrics = metrics["static"]
        # duration columns
        self.num_duration_cols = duration_cols["num_cols"]
        self.duration_col_names = duration_cols["col_names"]
        self.duration_col_total = duration_cols["total_col"]
        # column names
        self.col_names_by_time = col_names["by_time"]
        self.all_col_names = col_names["all"]
        # id columns (for updating queried cols)
        self.ensemble_col = id_cols["ensemble"]

    # given a df with a column titled "ensemble_uuid" and queried4 columns with json-like data
    # return a df with all queried columns updated to be a single float value
    def update_queried_cols(self, df):
        # update columns - sum non_static columns, get values for static columns
        df = update_columns(df, self.all_col_names, self.ensemble_col, no_sum_metrics=self.static_metrics)
        return df

    # given a df, insert percent columns for every duration column
    def add_percent_columns(self, df):
        # metrics lists that will be used to get/calculate percentages
        percent_metrics = ["cpu_request_%", "mem_request_%"]  # these do not exist yet - the columns for these metrics will be calculated
        numerator_metrics = ["cpu_usage", "mem_usage"]
        denominator_metrics = ["cpu_request", "mem_request"]  # cpu_request and mem_request

        # insert percentage columns as df[percent_metric_col] = 100 * df[numerator_metric_col] / df[denominator_metric_col]
        df = insert_percent_cols(
            df, percent_metrics, numerator_metrics, denominator_metrics,
            self.static_metrics, self.num_duration_cols)

        return df

    # given a dataframe with the columns "cpu_usage_total" and "mem_usage_total", drop all rows where those columns are 0
    # return the updated dataframe
    def drop_zero_cpu_mem(self, df, reset_index=True):
        zero_mask = (df['cpu_usage_total'] == 0) | (df['mem_usage_total'] == 0)
        non_zeros = df[~zero_mask]
        if reset_index:
            non_zeros = non_zeros.reset_index(drop=True)
        return non_zeros

    # given a df and columns to drop (also a bool for reset_index),
    # drop the ccolumns from the df
    def drop_columns(self, df, columns_to_drop, reset_index=True):
        df = df.drop(columns=columns_to_drop)
        if reset_index:
            df = df.reset_index(drop=True)
        return df

    # given an i (1 through num_duration_cols inclusive), 
    # return 
        # insert_ratio_cols - a list of ratio column names at i to be inserted
        # numerator_cols - a list of numerator column names at i to use for calculation (that already exist)
        # duration_col - a column name of the duration column at i
    def _get_ratio_components(self, i):
        if i < 1 or i > self.num_duration_cols:
            raise ValueError("i should be an int between 1 and num_duration_cols inclusive.")

        # get numerator column names, new insert column names, and duration column name
        numerator_cols = [name for name in self.col_names_by_time if name[-1] == str(i)]
        insert_ratio_cols = [f"{name}_ratio" for name in numerator_cols]
        duration_col = f"duration_t{i}"

        return insert_ratio_cols, numerator_cols, duration_col

    # given a dataframe, return the updated dataframe 
    # with new columns inserted as a ratio of numerator_col/duration_col
    def insert_ratio_columns(self, df, drop_numerators=True, reset_index=True):
        # handle improper user input
        if reset_index and not drop_numerators:
            raise ValueError("reset_index can only be True if drop_numerators is also True")

        # get column names, then calculate and insert ratio columns
        for i in range(1, self.num_duration_cols+1):
            # get column names for ratio cols to be inserted, numerator cols, duration (denominator) col
            cols_to_insert, numerator_cols, duration_col = self._get_ratio_components(i)

            # calculate and insert ratio columns
            for insert_col, numerator_col in zip(cols_to_insert, numerator_cols):
                df[insert_col] = df[numerator_col].astype(float) / df[duration_col].astype(float)

            # drop numerator columns if requested
            if drop_numerators:
                df.drop(columns=numerator_cols)
                # reset index if requested
                if reset_index:
                    df = df.reset_index(drop=True)

        return df

    '''
    ====================================
            Main Program
    ====================================
    '''
    # runs the whole phase. Returns True if successful, False otherwise
    def run(self):
        success = False

        # get queried df from previous phase
        queried_df = pd.read_csv(self.files['read'], index_col=0)

        # sum over json to get floats for resource metrics
        summed_df = self.update_queried_cols(queried_df)

        # add in percent columns
        percents_included_df = self.add_percent_columns(summed_df)

        # drop rows with zeros in cpu & mem total
        nonzero_df = self.drop_zero_cpu_mem(percents_included_df, reset_index=True)

        # add ratio cols for duration_t_i columns and drop numerator columns of ratio cols
        ratios_added_df = self.insert_ratio_columns(nonzero_df, drop_numerators=True, reset_index=True)

        # drop newly unnecessary columns
        final_df = self.drop_columns(ratios_added_df, self.drop_cols, reset_index=True)

        # save df to a csv file
        final_df.to_csv(self.files['write'])
        print(final_df)
        
        success = True
        return success

