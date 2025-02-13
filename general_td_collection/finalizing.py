import ast
import warnings
import pandas as pd


class Finalizer():
    """Handles summation/synthesis of queried metrics

    Given a list of graph metrics (optional), work with dataframes to sum over their rows

    Attributes:
        graph_metrics: A list of metrics for graphs to be summarized by. For all possible
                       options, use Finalizer.get_graph_metrics_list()
    """

    def __init__(self, graph_metrics: list[str] | str | None = None) -> None:
        self._all_graph_metrics = ["min", "max", "mean", "median",
                                   "std", "var", "sum", "increase", "q1", "q3", "iqr"]
        if graph_metrics is None:
            graph_metrics = []
        else:
            self._check_graph_metrics(graph_metrics)
        self.graph_metrics = graph_metrics
        # NOTE: if any metric gets updated here, self._summarize_graph_data must also be updated
        self._read_file = "csvs/queried.csv"
        self._graph_columns = []  # they become initialized when self.sum_df is called
        self._static_columns = []  # they become initialized when self.sum_df is called

    def get_graph_metrics_list(self):
        """Get a list of all available metrics to describe graph queries"""
        return self._all_graph_metrics.copy()

    def get_graph_columns(self, df: pd.DataFrame) -> list[str]:
        """Get the names of columns in a dataframe that contain graph metrics.

        Given a dataframe with some columns prepended with 'graph_', 
        return a list of all those column names.

        Args:
            df: a dataframe containing queried information 

        Returns:
            A list of column names
       """
        prefix = "graph_"
        graph_columns = [
            col for col in df.columns if col[:len(prefix)] == prefix]
        return graph_columns
    
    def get_static_columns(self, df: pd.DataFrame) -> list[str]:
        """Given a dataframe with some columns prepended with 'static_', 
        return a list of all those column names."""
        prefix = "static_"
        static_graph_columns = [
            col for col in df.columns if col[:len(prefix)] == prefix]
        return static_graph_columns

    def sum_df(self, df: pd.DataFrame, graph_metrics: list[str] | None = None) -> pd.DataFrame:
        """Sum over a dataframe to go from queried json data to single datapoints in columns

        Args:
            df: pandas DataFrame containing 'start', 'end', and queried columns
            graph_metrics: list of metrics for graphs to be summarized by 
                - for list of all metrics, look at self.all_graph_metrics

        Returns:
            Summed dataframe with new columns and some dropped old ones.
        """
        wrong_type_msg = f"Expected df to be a pandas DataFrame but was type {type(df)}"
        assert isinstance(df, pd.DataFrame), wrong_type_msg
        self._graph_columns = self.get_graph_columns(df)
        self._static_columns = self.get_static_columns(df)
        sum_col_prefix = "queried_"
        sum_columns = [
            col for col in df.columns if col[:len(sum_col_prefix)] == sum_col_prefix]
        for col_name in sum_columns:
            summed_col = col_name[len(sum_col_prefix):]
            df[summed_col] = df[col_name].apply(self._sum_result_list)
        # drop queried columns
        df = df.drop(columns=sum_columns)
        # since each cell in a graph_column contains many datapoints,
        # insert metric columns to summarize them, then drop the original graph columns
        df = self._insert_graph_metric_columns(
            df, graph_metrics)
        df = df.drop(columns=self._graph_columns)
        df = self._sum_static_columns(df)
        df = df.drop(columns=self._static_columns)
        return df

    # given a list of graph columns (strings), check that it is the right type
    def _check_graph_columns(self, graph_columns: list[str] | str) -> None:
        # if there's just one passed in column, still put it in a list
        if isinstance(graph_columns, str):
            graph_columns = [graph_columns]
        wrong_type_msg = f"graph_columns must be a str or list but was {type(graph_columns)}."
        assert isinstance(graph_columns, list), wrong_type_msg

    # given a dict of metrics and statuses (booleans),
    # check that all the keys are recognized and it is of the right type.
    def _check_graph_metrics(self, graph_metrics: list[str] | str) -> None:
        wrong_type_msg = f"graph_metrics must be a str or list of strs, with any of the following possible elements: {self._all_graph_metrics}"
        assert isinstance(graph_metrics, (list, str)), wrong_type_msg

        if len(graph_metrics) == 0:
            warnings.warn(
                "graph_metrics list is empty - no information will be saved from graph queries")
            return
        if isinstance(graph_metrics, str):
            graph_metrics = [graph_metrics]
        for metric in graph_metrics:
            if metric not in self._all_graph_metrics:
                raise ValueError(
                    f"metric '{metric}' not in acceptable metrics: {self._all_graph_metrics}.")

    # given a title that may have capitals and spaces,
    # return a lowercase version, with all spaces replaced with underscores
    def _title_to_col_name(self, title: str) -> str:
        title = title.lower()
        title = title.replace(" ", "_")
        return title

    # given a title of a graph and a metric, return a title of metric_graph_title,
    # with everything lowercase and no spaces (spaces replaced with underscores)
    def _get_metric_col_name(self, graph_title: str, metric: str) -> str:
        if metric not in self._all_graph_metrics:
            raise ValueError(
                f"metric {metric} not in acceptable metrics: {self.graph_metrics}")
        prefix = "graph_"
        no_prefix_title = graph_title[len(prefix):]
        cleaned_title = self._title_to_col_name(no_prefix_title)
        col_name = f"{metric}_{cleaned_title}"
        return col_name

    # takes in a list containing graph data (result list from a query) and a metric
    # returns the metric taken of the series, e.g. if metric is "std", takes the standard deviation
    def _summarize_graph_data(self, graph_data_list: pd.DataFrame, metric: str) -> float:
        if not isinstance(graph_data_list, list) and pd.isna(graph_data_list):
            return None
        if isinstance(graph_data_list, str):
            graph_data_list = ast.literal_eval(graph_data_list)
        graph_data = pd.Series(data=graph_data_list)
        # NOTE: if any metric gets updated here, self.all_graph_metrics must also be updated
        calculated_metric = 0
        match metric:
            case "mode":
                calculated_metric = graph_data.mode()
            case "min":
                calculated_metric = graph_data.min()
            case "max":
                calculated_metric = graph_data.max()
            case "mean":
                calculated_metric = graph_data.mean()
            case "std":
                calculated_metric = graph_data.std()
            case "var":
                calculated_metric = graph_data.var()
            case "sum":
                calculated_metric = graph_data.sum()
            case "q1":
                calculated_metric = graph_data.quantile(q=0.25)
            case "median":  # aka q2
                calculated_metric = graph_data.quantile(q=0.5)
            case "q3":
                calculated_metric = graph_data.quantile(q=0.75)
            case "iqr":
                q3 = graph_data.quantile(q=0.75)
                q1 = graph_data.quantile(q=0.25)
                calculated_metric = q3 - q1
            case "increase":
                first_point = graph_data.iloc[0]
                final_point = graph_data.iloc[len(graph_data) - 1]
                calculated_metric = final_point - first_point
            case _:
                raise ValueError(
                    f"metric {metric} not in known metrics: {self._all_graph_metrics}")
        return calculated_metric

    # given a dataframe with some columns starting with '_graph',
    # return a new df with more columns that summarize those graph columns
    def _insert_graph_metric_columns(self, df: pd.DataFrame, graph_metrics: list[str] | None = None) -> pd.DataFrame:
        # handle user input
        assert isinstance(
            df, pd.DataFrame), f"df must be a pandas dataframe but was {type(df)}"

        if graph_metrics is None:
            graph_metrics = self.graph_metrics
        else:
            self._check_graph_metrics(graph_metrics)

        # calculate and insert metric columns
        for graph_title in self._graph_columns:
            for graph_metric in graph_metrics:
                metric_col_name = self._get_metric_col_name(
                    graph_title, graph_metric)
                # make sure metric is passed in by value, not reference (cell-var-from-loop warning)
                df[metric_col_name] = df[graph_title].apply(
                    lambda cell, metric=graph_metric: self._summarize_graph_data(cell, metric))
        return df
    
    def _sum_static_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for static_title in self._static_columns:
            prefix = 'static_'
            updated_title = static_title[len(prefix):]
            df[updated_title] = df[static_title].apply(
                lambda cell, metric='mode': self._summarize_graph_data(cell, metric))
        return df

    # given a result list from a query, sum it to get the result
    def _sum_result_list(self, result_list: list[dict] | str) -> float:
        total = 0
        if isinstance(result_list, str):
            result_list = ast.literal_eval(result_list)
        for metric_value_dict in result_list:
            time_value_pair = metric_value_dict['value']
            total += float(time_value_pair[1])
        return total
