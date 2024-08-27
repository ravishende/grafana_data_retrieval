import shutil
from datetime import datetime, timedelta
import pandas as pd

# modules
from tables import Tables
from helpers.time_functions import delta_to_time_str, datetime_ify, calculate_offset


class TableQueryer():
    """Meant for querying data for tables metrics"""

    def __init__(self, namespace: str = "wifire-quicfire") -> None:
        self.namespace = namespace
        self._tables_class = Tables(namespace=namespace)

    # given a dataframe of runs, and list of indices of runs in the dataframe, (can also specify as_one_df and only_include_worker_pods)
    # return a dataframe (if as_one_df==True) of all of the tables queried for those runs
    # or return a list of dataframes (if_as_one_df==False) of all the tables queried for that run
    def get_tables_for_many_runs(self, runs_df: pd.DataFrame, run_indices: list[int], as_one_df: bool = False, only_include_worker_pods: bool = False) -> list[pd.DataFrame]:
        # get run start times as datetimes
        runs_df['start'] = runs_df['start'].apply(datetime_ify)

        # get all the selected runs into a single df to iterate over
        selected_runs_df = runs_df.iloc[run_indices]

        # get tables as a single dataframe for each run, add it to dfs_list
        dfs_list = []
        for index, run in selected_runs_df.iterrows():
            # get duration and start of run
            start = run['start']
            duration_seconds = run['runtime']

            # get queries and partial_queries to be passed into tables_class methods
            run_queries, run_partial_queries = self._get_queries(
                start, duration_seconds)

            # get tables as one df from queries
            tables_df = self._tables_class.get_tables_as_one_df(
                only_include_worker_pods=only_include_worker_pods,
                queries=run_queries,
                partial_queries=run_partial_queries)
            # fill in missing values in requests and limits
            tables_df = self._fill_in_static_na(tables_df, "cpu")
            tables_df = self._fill_in_static_na(tables_df, "mem")

            # add tables_df to dfs_list
            tables_df = self._rename_tables(tables_df)
            tables_df.insert(0, 'run_index', index)
            if 'run_uuid' in run:
                tables_df.insert(0, 'run_id', run['run_uuid'])
            dfs_list.append(tables_df)

        # get all runs as a single dataframe
        if as_one_df:
            table_runs_df = pd.concat(dfs_list, ignore_index=True)
            return table_runs_df

        # otherwise, return a list of dataframes
        return dfs_list

    # given a dataframe of runs, and an index of a run in the dataframe, (can also specify as_one_df and only_include_worker_pods)
    # return a dataframe (if as_one_df==True) of all of the tables queried for that run
    # or return a dict of dataframes (if_as_one_df==False) of all the tables queried for that run separated by table type
    def get_tables_for_one_run(
            self, runs_df: pd.DataFrame, run_index: int, as_one_df: bool = False, only_include_worker_pods: bool = False) -> list[pd.DataFrame]:
        # get tables class to be able to use methods for querying tables
        tables_class = Tables(namespace=self.namespace)

        # get run start times as datetimes and select run to use
        runs_df['start'] = runs_df['start'].apply(datetime_ify)
        run = runs_df.iloc[run_index]

        # get duration and start of run
        start = run['start']
        duration_seconds = run['runtime']

        # get queries and partial_queries to be passed into tables_class methods
        run_queries, run_partial_queries = self._get_queries(
            start, duration_seconds)

        # get tables as one df from queries
        if as_one_df:
            tables_df = tables_class.get_tables_as_one_df(
                only_include_worker_pods=only_include_worker_pods,
                queries=run_queries,
                partial_queries=run_partial_queries)
            # fill in missing values in requests and limits
            tables_df = self._fill_in_static_na(tables_df, "cpu")
            tables_df = self._fill_in_static_na(tables_df, "mem")
            # rename tables
            tables_df = self._rename_tables(tables_df)
            return tables_df

        # otherwise get tables as dict of dfs
        tables_dict = tables_class.get_tables_dict(
            only_include_worker_pods=only_include_worker_pods,
            queries=run_queries,
            partial_queries=run_partial_queries)
        # fill in missing values in requests and limits
        tables_dict['CPU Quota'] = self._fill_in_static_na(
            tables_dict['CPU Quota'], "cpu")
        tables_dict['Memory Quota'] = self._fill_in_static_na(
            tables_dict['Memory Quota'], "mem")
        # rename tables
        tables_dict = self._rename_tables(tables_dict)

        return tables_dict

    def _get_static_query_bodies(self) -> dict[str, str]:
        # Static Metrics: metrics that never change throughout a run, e.g. Memory Limits
        static_query_bodies = {
            'CPU Requests': 'cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests',
            'CPU Limits': 'cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits',
            'Memory Requests': 'cluster:namespace:pod_memory:active:kube_pod_container_resource_requests',
            'Memory Limits': 'cluster:namespace:pod_memory:active:kube_pod_container_resource_limits'
        }
        return static_query_bodies

    def _get_max_query_bodies(self) -> dict[str, str]:
        # Max Over Time Range Metrics:
        # Metrics that fluctuate over a run but only max val matters, e.g. Memory Usage
        max_query_bodies = {
            'Memory Usage': 'container_memory_working_set_bytes',
            'Memory Usage (RSS)': 'container_memory_rss',
            'Memory Usage (Cache)': 'container_memory_cache'
        }
        return max_query_bodies

    def _get_increase_query_bodies(self):
        # Increasing Metrics: metrics that increase over a run, e.g. CPU Usage
        increase_query_bodies = {
            'CPU Usage': 'container_cpu_usage_seconds_total',
            'Current Receive Bandwidth': 'container_network_receive_bytes_total',
            'Current Transmit Bandwidth': 'container_network_transmit_bytes_total',
            'Rate of Received Packets': 'container_network_receive_packets_total',
            'Rate of Transmitted Packets': 'container_network_transmit_packets_total',
            'Rate of Received Packets Dropped': 'container_network_receive_packets_dropped_total',
            'Rate of Transmitted Packets Dropped': 'container_network_transmit_packets_dropped_total',
            'IOPS(Reads)': 'container_fs_reads_total',
            'IOPS(Writes)': 'container_fs_writes_total',
            'Throughput(Read)': 'container_fs_reads_bytes_total',
            'Throughput(Write)': 'container_fs_writes_bytes_total'
        }
        return increase_query_bodies

    def _get_metrics_by_type(self) -> dict[str, str]:
        full_metrics = {
            # CPU Quota
            'CPU Usage',
            'CPU Requests',
            'CPU Limits',
            # Memory Quota
            'Memory Usage',
            'Memory Requests',
            'Memory Limits',
            'Memory Usage (RSS)',
            'Memory Usage (Cache)',
            # Network Usage
            'Current Receive Bandwidth',
            'Current Transmit Bandwidth',
            'Rate of Received Packets',
            'Rate of Transmitted Packets',
            'Rate of Received Packets Dropped',
            'Rate of Transmitted Packets Dropped'
        }
        partial_metrics = {
            # Input Output
            'IOPS(Reads)',
            'IOPS(Writes)',
            'Throughput(Read)',
            'Throughput(Write)'
        }
        all_metrics = {
            'full': full_metrics,
            'partial': partial_metrics
        }
        return all_metrics

    # assemble queries for all max based metrics
    def _assemble_max_queries(self, start: datetime, duration_seconds: int) -> dict[str, str]:
        duration_seconds = int(duration_seconds)
        # get offset and duration for query
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))

        # get components of query ready to be assembled
        prefix = "sum by (node, pod) (max_over_time("
        suffix = '{namespace="' + self.namespace + \
            '"}[' + str(duration) + '] offset ' + str(offset) + '))'

        # assemble queries
        max_queries = {}
        max_query_bodies = self._get_max_query_bodies()
        for title, query_body in max_query_bodies.items():
            max_queries[title] = prefix + query_body + suffix

        return max_queries

    # assemble queries for all increase based metrics
    def _assemble_increase_queries(self, start: datetime, duration_seconds: int) -> dict[str, str]:
        duration_seconds = int(duration_seconds)
        # get offset and duration for query
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))

        # get components of query ready to be assembled
        prefix = "sum by (node, pod) (increase("
        suffix = '{namespace="' + self.namespace + \
            '"}[' + str(duration) + '] offset ' + str(offset) + '))'

        # assemble queries
        increase_queries = {}
        increase_query_bodies = self._get_increase_query_bodies()
        for title, query_body in increase_query_bodies.items():
            increase_queries[title] = prefix + query_body + suffix

        return increase_queries

    # assemble queries for all static metrics
    def _assemble_static_queries(self, start: datetime, duration_seconds: int) -> dict[str, str]:
        # get offset for query 5 seconds after run started instead of as run ends - fewer NA pods for limits and requests
        # to get offset for when the run ends, use `offset = calculate_offset(start, duration_seconds)`
        offset = calculate_offset(start, 5)

        # get prefix of query ready for assembly (suffix gets defined while looping over query_bodies)
        prefix = "sum by (node, pod) ("

        # get the right suffix depending on the resource
        suffixes = {
            # change metric from measuring cpu cores to cpu seconds by multiplying by seconds
            'cpu': '{resource="cpu", namespace="' + self.namespace + '"} offset ' + str(offset) + ') * ' + str(duration_seconds),
            'mem': '{resource="memory", namespace="' + self.namespace + '"} offset ' + str(offset) + ')'
        }

        # assemble queries
        static_queries = {}
        static_query_bodies = self._get_static_query_bodies()
        for title, query_body in static_query_bodies.items():
            # get the resource and corresponding suffix
            resource = title[:3].lower()
            suffix = suffixes[resource]
            # assemble query and put into static_queries
            static_queries[title] = prefix + query_body + suffix

        return static_queries

    # given a dict of all query bodies, a dict of all metrics, a run start (datetime), and run duration_seconds (int or float)
    # return two dictionaries, one with full queries, and one with partial queries, both to be passed into tables_class methods
    def _get_queries(self, start: datetime, duration_seconds: int) -> tuple[dict, dict]:
        # assemble queries
        max_queries = self._assemble_max_queries(start, duration_seconds)
        increase_queries = self._assemble_increase_queries(
            start, duration_seconds)
        static_queries = self._assemble_static_queries(start, duration_seconds)

        # get all queries into one dictionary
        unsorted_queries = {}
        unsorted_queries.update(max_queries)
        unsorted_queries.update(increase_queries)
        unsorted_queries.update(static_queries)

        # assemble queries and partial_queries for the run - sorted versions of the unsorted_queries separated by full queries vs partial_queries
        metrics_dict = self._get_metrics_by_type()
        run_queries = {key: unsorted_queries[key]
                       for key in metrics_dict['full']}
        run_partial_queries = {
            key: unsorted_queries[key] for key in metrics_dict['partial']}

        return run_queries, run_partial_queries

    # given a table dataframe of either cpu quota or memory quota, a resource ("mem" or "cpu"),
    # fill in the na values for requests, limits, requests %, and limits %
    def _fill_in_static_na(self, df: pd.DataFrame, resource: str) -> pd.DataFrame:
        # use resource to determine resource_str
        resource_str = ""
        if resource == "cpu":
            resource_str = "CPU"
        elif resource == "mem":
            resource_str = "Memory"
        else:
            raise ValueError('resource must be either "cpu" or "mem"')

        # fill in na values
        df[[f'{resource_str} Requests', f'{resource_str} Limits']] = df[[
            f'{resource_str} Requests', f'{resource_str} Limits']].fillna(method='ffill')
        df[f'{resource_str} Requests %'] = df[f'{resource_str} Usage'].astype(
            float) / df[f'{resource_str} Requests'].astype(float) * 100
        df[f'{resource_str} Limits %'] = df[f'{resource_str} Usage'].astype(
            float) / df[f'{resource_str} Limits'].astype(float) * 100

        return df

    # Returns a dataframe with changed names of certain columns to represent the true data for
    # a run rather than the current snapshot in time.
    # Note: The updated queries already query the correct information, we just have to change
    # the names to match, after we do the querying from the tables_class methods (which rely on the names).
    def _rename_df_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_dict = {}
        for old_name in df.columns:
            if "IOPS" in old_name:  # change "IOPS" column names to "IO"
                rename_dict[old_name] = old_name.replace("IOPS", "IO")
            elif "Current" in old_name:  # get rid of "Current"
                rename_dict[old_name] = old_name.replace("Current", "")
            elif "Rate of" in old_name:  # get rid of "Rate of"
                rename_dict[old_name] = old_name.replace("Rate of", "")
            else:  # keep old name
                rename_dict[old_name] = old_name

        renamed_df = df.rename(columns=rename_dict)
        return renamed_df

    # if tables is a single dataframe or list of dataframes, updates names of the metrics in tables.
    # if tables is a dict of dataframes, updates names of the tables and their metrics.
    # returns the orginal dataframe or dict of dataframes
    def _rename_tables(self, tables: pd.DataFrame | dict[str, pd.DataFrame] | list[pd.DataFrame]):
        # If tables is not one of the above, there is a wrong user input
        wrong_type_msg = "tables must either be a dataframe, a list of dataframes, or a dict of dataframes."
        assert isinstance(tables, (pd.DataFrame, dict, list)), wrong_type_msg

        # tables is a single dataframe
        if isinstance(tables, pd.DataFrame):
            return self._rename_df_metrics(tables)

        # tables is a dict of dataframes
        if isinstance(tables, dict):
            # rename tables titles ("Current Storage IO" --> "Storage IO", "Current Netowrk Usage" --> "Network Usage")
            tables_dict = tables
            tables_dict['Storage IO'] = tables_dict.pop('Current Storage IO')
            tables_dict['Network Usage'] = tables_dict.pop(
                'Current Network Usage')
            # rename metrics in dataframes
            for title, table_df in tables_dict.items():
                tables_dict[title] = self._rename_df_metrics(table_df)
            return tables_dict

        # tables is a list of dataframes
        if isinstance(tables, list):
            tables_list = []
            for table_df in tables:
                tables_list.append(self._rename_df_metrics(table_df))
            return tables_list


# ============================
#         Main Program
# ============================


if __name__ == "__main__":
    # display settings
    pd.set_option("display.max_columns", None)
    terminal_width = shutil.get_terminal_size().columns
    pd.set_option('display.width', terminal_width)
    pd.set_option('display.max_colwidth', 30)
    pd.set_option("display.max_rows", None)

    # settings
    NAMESPACE = 'wifire-quicfire'
    READ_FILE = "general_td_collection/csvs/read.csv"
    QUERY_MULTIPLE_RUNS = False

    # get a dataframe of runs
    runs_info_df = pd.read_csv(READ_FILE, index_col=0)
    table_queryer = TableQueryer(namespace=NAMESPACE)
    # get tables data for one run
    RUN_INDEX = 50  # can pick any run between 0 and len(df)-1 inclusive
    run_tables = table_queryer.get_tables_for_one_run(
        runs_df=runs_info_df,
        run_index=RUN_INDEX,
        as_one_df=True,  # if set to False, returns a dictionary of titles, tables
        # if set to True, only includes bp3d-worker pods and changes their name to be just their ensemble id
        only_include_worker_pods=False
    )
    print(run_tables)

    if QUERY_MULTIPLE_RUNS:
        # can pick any runs between 0 and len(df)-1 inclusive
        RUN_INDICES = [50, 60, 70]
        runs_tables_df = table_queryer.get_tables_for_many_runs(
            runs_df=runs_info_df,
            run_indices=RUN_INDICES,
            as_one_df=True,  # if set to False, returns a dictionary of titles, tables
            # if set to True, only includes bp3d-worker pods and changes their name to be just their ensemble id
            only_include_worker_pods=False
        )
        print(runs_tables_df)
