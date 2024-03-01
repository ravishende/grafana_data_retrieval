from workflow_files import PHASE_4_FILES
from metrics_and_columns_setup import METRICS, DURATION_COLS, COL_NAMES, ID_COLS

class Phase_4():
    def __init__(self, files=PHASE_4_FILES, metrics=METRICS, duration_cols=DURATION_COLS, col_names=COL_NAMES, id_cols=ID_COLS):
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
        self.col_names_total = col_names["total"]
        self.col_names_by_time = col_names["by_time"]
        self.all_col_names = col_names["all"]
        # id columns (for updating queried cols)
        self.ensemble_col = id_cols["ensemble"]

    # given a df with a column titled "ensemble_uuid" and queried columns with json-like data
    # return a df with all queried columns updated to be a single float value
    def update_queried_cols(self, df):
        # update columns - sum non_static columns, get values for static columns
        df = update_columns(df, self.non_static_metrics, self.ensemble_col, static=False)
        df = update_columns(df, self.static_metrics, self.ensemble_col, static=True)
        return df

    def add_percent_columns(self, df):
        # metrics lists that will be used to get/calculate percentages
        percent_metrics = ["cpu_request_%", "mem_request_%"]  # these do not exist yet - the columns for these metrics will be calculated
        numerator_metrics = ["cpu_usage", "mem_usage"]
        denominator_metrics = self.static_metrics

        # insert percentage columns as df[percent_metric_col] = 100 * df[numerator_metric_col] / df[denominator_metric_col]
        df = insert_percent_cols(
            df, percent_metrics, numerator_metrics, denominator_metrics, 
            self.num_duration_cols, self.static_metrics)

        return df

    # given a dataframe with the columns "cpu_usage_total" and "mem_usage_total", drop all rows where those columns are 0
    # return the updated dataframe
    def drop_zero_cpu_mem(self, df, reset_index=True):
        zero_mask = (summed['cpu_usage_total'] == 0) | (summed['mem_usage_total'] == 0)
        non_zeros = summed[~zero_mask]
        if reset_index:
            non_zeros = non_zeros.reset_index(drop=True)

    # given a df and columns to drop (also a bool for reset_index),
    # drop the ccolumns from the df
    def drop_columns(self, df, columns_to_drop, reset_index=True):
        df = df.drop(columns=columns_to_drop)
        if reset_index:
            df = df.reset_index(drop=True)
        return df

'''
-------
step 11 - add ratio cols for duration_t_i columns and drop numerator columns
-------
'''

    # given an i (1 through num_duration_cols inclusive), 
    # return 
        # insert_ratio_cols - a list of ratio column names at i to be inserted
        # numerator_cols - a list of numerator column names at i to use for calculation (that already exist)
        # duration_col - a column name of the duration column at i
    def _get_ratio_components(self, i):
        if i < 1 or i > self.num_duration_cols:
            raise ValueError("i should only be 1 through num_duration_cols inclusive.")

        # get non static metrics which will be used to find numerator columns
        _, non_static_metrics = _get_metrics()

        # get numerator column names, new insert column names, and duration column name
        numerator_cols = [f"{metric}_t{i}" for metric in non_static_metrics]
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
    def run(self):
        # 8. sum over json to get floats for resource metrics
        summed_df = self.update_queried_cols(queried_df)

        # 9. add in percent columns
        percents_included_df = self.add_percent_columns(summed_df)

        # 10. drop rows with zeros in cpu & mem total
        nonzero_df = self.drop_zero_cpu_mem(percents_included_df, reset_index=True)

        # 11. add ratio cols for duration_t_i columns and drop numerator columns of ratio cols
        ratios_added_df = self.insert_ratio_columns(nonzero_df, drop_numerators=True, reset_index=True)

        # 12. drop_cols_2
        final_df = self.drop_columns(ratios_added_df, drop_cols_2, reset_index=True)

        # save df to a csv file
        final_df.to_csv(phase4files['write'])

