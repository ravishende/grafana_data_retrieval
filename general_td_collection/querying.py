# autopep8: off
import sys
import os
import warnings
from datetime import timedelta
import pandas as pd
from termcolor import colored
from tqdm import tqdm
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# pylint: disable=wrong-import-position
from helpers.time_functions import calculate_offset, delta_to_time_str, datetime_ify
from helpers.querying import query_data
from graphs import Graphs
# autopep8: on


class QueryHandler():
    """Handles querying of information over time using several dashboards

    Given a read/write files for dataframes (optional), a graph timestep (optional) for how frequent
    graph datapoints should be queried for, and some way to filter queries, 
    work with dataframes to query data over their rows (assuming a 'start' and 'end' column exist)

    Note: the filter parameters are optional but highly recommended. If none of them are specified, 
    the queries will be over the entirity of the relevant data stored in nautilous, which will 
    almost certainly not be specific to the desired application being queried.

    Attributes:
        verbose: A boolean for if printing while querying should be verbose
    """

    # pylint: disable=too-many-arguments
    def __init__(self, read_file: str = "csvs/run_inputs.csv",
                 write_file: str = "csvs/queried.csv", graph_timestep: str = "1m",
                 node: str | None = None, node_regex: str | None = None,
                 pod: str | None = None, pod_regex: str | None = None,
                 namespace: str | None = None, namespace_regex: str | None = None,
                 verbose: bool = True, display_warnings: bool = True) -> None:
        if node and node_regex:
            raise ValueError(
                "At most one of node or node_regex can be defined")
        if pod and pod_regex:
            raise ValueError("At most one of pod or pod_regex can be defined")
        if namespace and namespace_regex:
            raise ValueError(
                "At most one of namespace or namespace_regex can be defined")
        self.verbose = verbose
        self.display_warnings = display_warnings
        # changes how many datapoints are in each queried graph - tunes accuracy of graph metrics
        self._graph_timestep = graph_timestep
        # string to insert into queries to filter them
        self._filter = ""
        self.update_filter(
            node, node_regex, pod, pod_regex, namespace, namespace_regex)
        # csv files
        self._read_file = read_file
        self._write_file = write_file
        self._progress_file = "csvs/_query_progress.csv"

    def update_filter(self, node: str | None = None, node_regex: str | None = None,
                      pod: str | None = None, pod_regex: str | None = None,
                      namespace: str | None = None, namespace_regex: str | None = None) -> None:
        """Updates the filter settings used in querying based on the passed in filter parameters

        Args:
            node: the node to filter on
            node_regex: regex pattern to include any nodes that match the pattern
            pod: the pod to filter on
            pod_regex: regex pattern to include any pods that match the pattern
            namespace: the namespace to filter on
            namespace_regex: regex pattern to include any namespaces that match the pattern

        Returns:
            None
        """
        # get individual filters
        node_filter = self._get_component_filter_str(
            "node", node, node_regex)
        pod_filter = self._get_component_filter_str(
            "pod", pod, pod_regex)
        namespace_filter = self._get_component_filter_str(
            "namespace", namespace, namespace_regex)

        # assemble filter str
        filters = [node_filter, pod_filter, namespace_filter]
        filter_str = ""
        for filt in filters:
            if filt == "":
                continue
            filter_str += filt + ', '

        # remove trailing comma
        if filter_str[-2:] == ', ':
            filter_str = filter_str[:-2]

        if filter_str != "":
            self._filter = filter_str
            return
        
        if not self.display_warnings:
            return

        warning_msg = (
            "No filters specified. This will likely cause any queried data to be inaccurate." +
            "You can either specify them while initializing QueryHandler or call" +
            "QueryHandler.update_filter, passing in the relevant arguments.")
        warnings.warn(colored(warning_msg, "red"))
        response = input(
            "\nContinue without filter? Type 'y' to continue.\n")
        if response != 'y':
            sys.exit()

    def query_df(self, df: pd.DataFrame | None = None, batch_size: int = 5,
                 rgw_queries: bool = False, gpu_queries: bool = False, gpu_compute_resource_queries: bool = False,
                 cpu_compute_resource_queries: bool = False) -> pd.DataFrame:
        """Insert queried columns into a dataframe based on chosen dashboards

        Args:
            df: pandas dataframe to query - must have a 'start' and 'end' column
            batch_size: how many rows to query for before saving - small numbers recommended
            rgw_queries: whether to query for rgw queue, cache, and gets/puts metrics
            gpu_queries: whether to query for total gpu usage and requested gpus
            gpu_compute_resource_queries: whether to query for gpu utilization and physical metrics
            cpu_compute_resource_queries: whether to query for cpu, memory and network metrics
        Returns:
            dataframe with more columns of queried information

        Raises:
            ValueError: invalid inputs - no queries specified or no/invalid df or read file
        """

        # handle user input
        if not (gpu_queries or gpu_compute_resource_queries or
                rgw_queries or cpu_compute_resource_queries):
            raise ValueError("No queries specified -> nothing to query")
        if df is None:
            try:
                pd.read_csv(self._read_file)
            except Exception as exc:
                raise ValueError(
                    f"No df passed in, and default read file ({self._read_file}) cannot be read: {exc}") from exc
        if len(df) == 0:
            print("df is empty")
            return df

        # set up dataframes - progress df and what's left to query
        queried_df = pd.DataFrame()
        df_to_query = pd.DataFrame()
        if os.path.exists(self._progress_file):
            try:
                queried_df = pd.read_csv(self._progress_file, index_col=0)
            except (pd.errors.EmptyDataError, pd.errors.ParserError):
                pass

        if len(queried_df) > 0:
            queried_df = queried_df.reset_index(drop=True)
            df_to_query = df.iloc[len(queried_df):].reset_index(drop=True)
        else:
            df_to_query = df.reset_index(drop=True)

        if len(df_to_query) > 0:
            df_to_query = self._preprocess_df(df_to_query)
        # get queries
        graph_queries = self._get_graph_queries(
            gpu_queries=gpu_queries, gpu_compute_resource_queries=gpu_compute_resource_queries, rgw_queries=rgw_queries,
        )
        # since non-graph queries require start and end times embedded in the queries,
        # pass them in as functions
        non_graph_query_functions = []
        if cpu_compute_resource_queries:
            non_graph_query_functions.append(
                self._get_cpu_compute_resource_queries)

        static_graph_queries = self._get_static_graph_queries(
            cpu_compute_resource_queries=cpu_compute_resource_queries)

        graphs_class = Graphs(time_step=self._graph_timestep)

        # query in batches
        for batch_start in range(0, len(df_to_query), batch_size):
            batch_end = min(batch_start+batch_size, len(df_to_query))
            df_chunk = df_to_query.iloc[batch_start:batch_end]
            print(
                colored(f"Querying rows {batch_start} to {batch_end-1}", "green"))
            # query graphs
            if self.verbose and len(graph_queries) > 0:
                print("querying graph information")
            df_chunk = df_chunk.apply(self._query_row_for_graphs,
                                      args=(graphs_class, graph_queries), axis=1)
            # query non graphs
            if self.verbose and len(non_graph_query_functions) > 0:
                print("querying non-graph information")
            df_chunk = df_chunk.apply(lambda row: self._query_row_for_non_graphs(
                row, non_graph_query_functions), axis=1)
            # query static graphs\
            is_static = True
            df_chunk = df_chunk.apply(self._query_row_for_graphs,
                                      args=(graphs_class, static_graph_queries, is_static), axis=1)
            # update df
            queried_df = pd.concat(
                [queried_df, df_chunk]).reset_index(drop=True)
            queried_df.to_csv(self._progress_file)
        # print(queried_df)
        queried_df.to_csv(self._write_file)
        return queried_df

    # given a kubernetes component ('node', 'pod', 'namespace', etc.) and the name (optional)
    # of the component (e.g. 'bp3d-worker-pod-a5343...') or a regex expression (optional)
    # for the component name,
    # return a string filtering for that component, e.g. 'pod="bp3d-worker-pod-a5343..."'
    def _get_component_filter_str(
            self, component: str, component_name: str = None, component_regex: str = None) -> str:
        if component_name and component_regex:
            raise ValueError(
                "at most one of component_name or component_regex should be defined.")
        # give a warning if it doesn't seem like the filter str is being used correctly
        known_k8s_components = ['node', 'pod', 'namespace', 'cluster', 'job', 'instance',
                                'instance_id', 'container', 'Hostname', 'UUID', 'device',
                                'endpoint', 'service', 'prometheus', 'service']
        if component not in known_k8s_components:
            warnings.warn(
                f"Unknown component '{component}'. Known components are: {known_k8s_components}")
        # give the string depending on if it's a regex expression or not
        if component_name:
            return f'{component}="{component_name}"'
        if component_regex:
            return f'{component}=~"{component_regex}"'
        # neither are defined
        return ""

    def _preprocess_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if not 'start' in df.columns or not 'end' in df.columns:
            raise ValueError(
                "dataframe must have a 'start' column and an 'end' columnn")
        # deal with times and create runtime column
        df['start'] = df['start'].apply(datetime_ify)
        df['end'] = df['end'].apply(datetime_ify)
        df['runtime'] = (df['end'] - df['start']).dt.total_seconds()
        df['runtime'] = df['runtime'].round()
        return df

    def _get_gpu_queries(self) -> dict[str:str]:
        # graph queries
        queries = {
            'total_gpu_usage': 'avg_over_time(namespace_gpu_utilization{' + self._filter + '}',
            'requested_gpus': 'count(DCGM_FI_DEV_GPU_TEMP{' + self._filter + '})'
        }
        queries = {}
        return queries

    def _get_gpu_compute_resource_queries(self) -> dict[str, str]:
        # graph queries
        queries = {
            'gpu_utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self._filter + '}',
            'memory_copy_utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self._filter + '}',
            'power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self._filter + '}',
            'temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self._filter + '}',
            'fan_speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self._filter + '}'
        }
        return queries

    def _get_rgw_queries(self) -> dict[str, str]:
        if "node=" in self._filter:
            specifier = ""
            if "node=~" in self._filter:
                specifier = "node_regex"
            else:
                specifier = "node"
            warnings.warn(
                f"'{specifier}' specified in filtering, but rgw queries don't have node data, resulting in no data returned for them.")
        # graph queries
        queries = {
            'rgw_queue_length': 'sum(ceph_rgw_qlen{' + self._filter + '})',
            'rgw_cache_hit': 'sum(ceph_rgw_cache_hit{' + self._filter + '})',
            'rgw_cache_miss': 'sum(ceph_rgw_cache_miss{' + self._filter + '})',
            'rgw_gets': 'sum(ceph_rgw_get{' + self._filter + '})',
            'rgw_puts': 'sum(ceph_rgw_put{' + self._filter + '})',
            'rgw_failed_req': 'sum(ceph_rgw_failed_req{' + self._filter + '})'
        }

        return queries

    # queries from ../training_data_handling/work_flow.py
    def _get_cpu_compute_resource_queries(self, start, end) -> dict[str, str]:
        start = datetime_ify(start)
        end = datetime_ify(end)
        duration_seconds = int((end-start).total_seconds())
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))

        # all resources and the heart of their queries
        queries = {
            # non static metrics
            'mem_usage': 'sum(max_over_time(container_memory_working_set_bytes{' + self._filter + '}[' + duration + '] offset ' + offset + '))',

            'cpu_usage': 'sum(increase(container_cpu_usage_seconds_total{' + self._filter + '}[' + duration + '] offset ' + offset + '))',

            'transmitted_packets': 'sum(increase(container_network_transmit_packets_total{' + self._filter + '}[' + duration + '] offset ' + offset + '))',

            'received_packets': 'sum(increase(container_network_receive_packets_total{' + self._filter + '}[' + duration + '] offset ' + offset + '))',

            'transmitted_bandwidth': 'sum(increase(container_network_transmit_bytes_total{' + self._filter + '}[' + duration + '] offset ' + offset + '))',

            'received_bandwidth': 'sum(increase(container_network_receive_bytes_total{' + self._filter + '}[' + duration + '] offset ' + offset + '))'
        }

        return queries

    def _get_graph_queries(self, gpu_queries: bool = False,
                           gpu_compute_resource_queries: bool = False,
                           rgw_queries: bool = False) -> dict[str, str]:
        graph_queries = {}

        if gpu_queries:
            new_queries = self._get_gpu_queries()
            graph_queries.update(new_queries)
        if gpu_compute_resource_queries:
            new_queries = self._get_gpu_compute_resource_queries()
            graph_queries.update(new_queries)
        if rgw_queries:
            new_queries = self._get_rgw_queries()
            graph_queries.update(new_queries)

        return graph_queries

    def _get_static_graph_queries(self, cpu_compute_resource_queries: bool = False):
        static_graph_queries = {}
        if cpu_compute_resource_queries:
            new_queries = {
                'cpu_request': 'sum by (pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{resource="cpu", ' + self._filter + '})',
                'mem_request': 'sum by (pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{resource="memory", ' + self._filter + '})'
            }
            static_graph_queries.update(new_queries)
        return static_graph_queries

    # given a row of a dataframe, a graphs class instantiation, and a dict of queries for graphs,
    # return a queried version of that row,
    # with the new columns being the graph titles prepended with 'graph_'
    def _query_row_for_graphs(
            self, row: pd.Series, graphs_class: Graphs, graph_queries: dict[str, str], static: bool = False) -> pd.Series:
        assert isinstance(row, pd.Series), "row must be a pandas Series"
        assert isinstance(graphs_class, Graphs), "graphs_class must be of type Graphs"
        assert isinstance(static, bool), "static must be a boolean"
        # sum by none since we eventually get it to one datapoint --> no need to split it further
        graphs_dict = graphs_class.get_graphs_from_queries(
            queries_dict=graph_queries, sum_by=None, start=row['start'], end=row['end'])
        for title, data in graphs_dict.items():
            new_title = ""
            if static:
                new_title = "static_" + title
            else:
                new_title = "graph_" + title
            if data is not None:
                data = data[title].astype(float).to_list()
            row[new_title] = data
        return row

    def _query_row_for_non_graphs(self, row: pd.Series, query_retrieval_funcs_list: list):
        if len(query_retrieval_funcs_list) == 0:
            return row
        data_dict = {}
        for query_retrieval_func in tqdm(query_retrieval_funcs_list):
            queries = query_retrieval_func(row['start'], row['end'])
            data_dict.update({title: query_data(query)
                              for title, query in queries.items()})
        # prepend a string for finalizing.py to know that the column needs to be summed
        for title, data in data_dict.items():
            new_title = "queried_" + title
            row[new_title] = data
        return row
