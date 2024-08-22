import pandas as pd
import warnings
import ast


class Finalizer():
    def __init__(self, graph_metrics: list[str] | str | None = None, graph_columns: list[str] | str = None) -> None:
        if graph_columns is not None:
            self._check_graph_columns(graph_columns)
        if graph_metrics is None:
            graph_metrics = []
        else:
            self._check_graph_metrics(graph_metrics)
        self.graph_metrics = graph_metrics
        self.graph_columns = graph_columns
        self.all_graph_metrics = ["min", "max", "mean", "median",
                                  "std", "var", "sum", "increase", "q1", "q3", "iqr"]

        self._read_file = "csvs/queried.csv"

    def get_graph_columns(self, df: pd.DataFrame) -> list[str]:
        prefix = "graph_"
        graph_columns = [
            col for col in df.columns if col[:len(prefix)] == prefix]
        return graph_columns

    # given a list of graph columns (strings), check that it is the right type
    def _check_graph_columns(self, graph_columns: list[str]) -> None:
        if isinstance(graph_columns, str):
            # if there's just one passed in column, put it in a list still
            graph_columns = [graph_columns]
        if not isinstance(graph_columns, list):
            raise ValueError(
                f"graph_columns must be a str or list but was {type(graph_columns)}.")

    # given a dict of metrics and statuses (booleans), check that all the keys are recognized and it is of the right type.
    def _check_graph_metrics(self, graph_metrics: list[str]) -> None:
        if graph_metrics != []:
            if not isinstance(graph_metrics, list) and not isinstance(graph_metrics, str):
                raise ValueError(
                    f"graph_query_metrics must be a string or list of strings, with the following possible elements: {self.all_graph_metrics}")
            if len(graph_metrics) == 0:
                warnings.warn(
                    "graph_metrics list is empty - no information will be saved from graph queries")
            for metric in graph_metrics:
                if metric not in self.all_graph_metrics:
                    raise ValueError(
                        f"metric '{metric}' not in acceptable metrics: {self.all_graph_metrics}.")

    # given a title that may have capitals and spaces,
    # return a lowercase version, with all spaces replaced with underscores
    def _title_to_col_name(self, title: str) -> str:
        title = title.lower()
        title = title.replace(" ", "_")
        return title

    # given a title of a graph and a metric, return a title of metric_graph_title,
    # with everything lowercase and no spaces (spaces replaced with underscores)
    def _get_metric_col_name(self, graph_title: str, metric: str) -> str:
        if metric not in self.all_graph_metrics:
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
        # NOTE: if anything gets updated here, self.all_graph_metrics must also be updated
        match metric:
            case "min":
                return graph_data.min()
            case "max":
                return graph_data.max()
            case "mean":
                return graph_data.mean()
            case "std":
                return graph_data.std()
            case "var":
                return graph_data.var()
            case "sum":
                return graph_data.sum()
            case "q1":
                return graph_data.quantile(q=0.25)
            case "median":  # aka q2
                return graph_data.quantile(q=0.5)
            case "q3":
                return graph_data.quantile(q=0.75)
            case "iqr":
                return graph_data.quantile(q=0.75) - graph_data.quantile(q=0.25)
            case "increase":
                return graph_data.iloc[len(graph_data) - 1] - graph_data.iloc[0]
            case _:
                raise ValueError(
                    f"metric {metric} not in known metrics: {self.all_graph_metrics}")

    # given a dataframe with some columns starting with '_graph',
    # return a new df with more columns that summarize those graph columns
    def _insert_graph_metric_columns(self, df: pd.DataFrame, graph_metrics: list[str] = None) -> pd.DataFrame:
        # handle user input
        if not isinstance(df, pd.DataFrame):
            raise ValueError(
                f"df must be a pandas dataframe but was {type(df)}")
        if graph_metrics is None:
            graph_metrics = self.graph_metrics
        else:
            self._check_graph_metrics(graph_metrics)

        # calculate and insert metric columns
        for graph_title in self.graph_columns:
            for metric in graph_metrics:
                metric_col_name = self._get_metric_col_name(
                    graph_title, metric)
                df[metric_col_name] = df[graph_title].apply(
                    lambda cell: self._summarize_graph_data(cell, metric))
        return df

    # given a result list from a query, sum it to get the result
    def _sum_result_list(self, result_list: list[dict]) -> float:
        total = 0
        if isinstance(result_list, str):
            result_list = ast.literal_eval(result_list)
        for metric_value_dict in result_list:
            time_value_pair = metric_value_dict['value']
            total += float(time_value_pair[1])
        return total

    def sum_df(self, df, graph_metrics: list[str] = None) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame):
            raise ValueError(
                f"Expected df to be a pandas DataFrame but was type {type(df)}")
        self.graph_columns = self.get_graph_columns(df)
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
        df = df.drop(columns=self.graph_columns)
        return df
