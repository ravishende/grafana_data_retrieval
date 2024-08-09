import pandas as pd


class Finalizer():
    def __init__(self, graph_metrics_dict: dict[str, bool] = None, graph_columns: list[str] = None) -> None:
        if graph_columns is None:
            graph_columns = self.get_graph_columns()

        self._check_graph_metrics_dict(graph_metrics_dict)
        self._check_graph_columns(graph_columns)
        self._graph_metric_cols_dict = {}
        self.graph_metrics_dict = graph_metrics_dict
        self.graph_columns = graph_columns
        self.all_graph_metrics = ['min', 'max', 'avg',
                                  'std', 'var', 'med', 'q1', 'q3', 'iqr']

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
    def _check_graph_metrics_dict(self, graph_metrics_dict: dict[str, bool]) -> None:
        if graph_metrics_dict is not None:
            if not isinstance(graph_metrics_dict, dict):
                raise ValueError(
                    "graph_query_metrics must be a dictionary of str:bool, where the possible keys are: ")
            for key in graph_metrics_dict.keys():
                if key not in self.all_graph_metrics:
                    raise ValueError(
                        f"key {key} not in acceptable metrics: {self.all_graph_metrics}.")

    # given a title that may have capitals and spaces, return a lowercase version, with all spaces replaced with underscores
    def _title_to_col_name(self, title: str) -> str:
        title = title.lower()
        title = title.replace(" ", "_")
        return title

    # given a title of a graph and a metric, return a title of metric_graph_title, with everything lowercase and no spaces (spaces replaced with underscores)
    def _get_metric_col_name(self, graph_title: str, metric: str) -> str:
        if metric not in self.graph_metrics_dict.keys():
            raise ValueError(
                f"metric {metric} not in acceptable metrics: {self.graph_metrics_dict.keys()}")
        cleaned_title = self._title_to_col_name(graph_title)
        col_name = f"{metric}_{cleaned_title}"
        return col_name

    # takes in a list containing graph data (result list from a query) and metric
    # returns the metric taken of the series e.g. if metric is "std", takes the standard deviation
    def summarize_graph_data(self, graph_data_list: list, metric: str) -> float:
        graph_values = [float(datapoint['value'][0])
                        for datapoint in graph_data_list]
        graph_data = pd.Series(graph_values)
        ['min', 'max', 'avg', 'std', 'var', 'med', 'q1', 'q3', 'iqr']
        match metric:
            case "min":
                return graph_data.min()
            case "max":
                return graph_data.max()
            case "avg":
                return graph_data.avg()
            case "std":
                return graph_data.std()
            case "var":
                return graph_data.var()
            case "med":  # aka q2
                return graph_data.quantile(q=0.5)
            case "q1":
                return graph_data.quantile(q=0.25)
            case "q3":
                return graph_data.quantile(q=0.75)
            case "iqr":
                return graph_data.quantile(q=0.75) - graph_data.quantile(q=0.25)
            case _:
                raise ValueError(
                    f"metric {metric} not in known metrics: {self.all_graph_metrics}")

    # given a dataframe with some columns starting with '_graph', return a new df with more columns that summarize those graph columns
    def _insert_graph_metric_columns(self, df: pd.DataFrame, graph_metrics_dict: dict[str, bool] = None) -> pd.DataFrame:
        # handle user input
        if not isinstance(df, pd.DataFrame):
            raise ValueError(
                f"df must be a pandas dataframe but was {type(df)}")
        if graph_metrics_dict is None:
            graph_metrics_dict = self.graph_metrics_dict
        else:
            self._check_graph_metrics_dict(graph_metrics_dict)

        # calculate and insert metric columns
        graph_metric_cols_dict = {title: [] for title in self.graph_columns}
        for graph_title in self.graph_columns:
            for metric, status in graph_metrics_dict.items():
                if status == False:
                    continue
                metric_col_name = self._get_metric_col_name(
                    graph_title, metric)
                graph_metric_cols_dict[graph_title].append(metric_col_name)
                df[metric_col_name] = df[graph_title].apply(
                    self.summarize_graph_data, args=(metric))

        self._graph_metric_cols_dict = graph_metric_cols_dict
        return df

    # given a result list from a query, sum it to get the result
    def _sum_result_list(self, result_list: list[dict]) -> float:
        total = 0
        for metric_value_dict in result_list:
            for time_value_pair in metric_value_dict['values']:
                total += time_value_pair[1]
        return total

    def sum_df(self, df, graph_metrics_dict: dict[str, bool] = None) -> pd.DataFrame:
        for column in df.columns:
            if column not in self.graph_columns:
                df[column] = df[column].apply(self._sum_result_list)

        # since each cell in a graph_column contains many datapoints,
        # insert metric columns to summarize them, then drop the original graph columns
        df = self._insert_graph_metric_columns(
            graph_metrics_dict, self.graph_columns)
        df = df.drop(columns=self.graph_columns)
        return df
