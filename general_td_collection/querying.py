# autopep8: off
from datetime import timedelta
import pandas as pd
import warnings
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.time_functions import calculate_offset, delta_to_time_str, datetime_ify
from graphs import Graphs
# autopep8: on


class Query_handler():
    def __init__(self, read_file: str = "csvs/run_inputs.csv", node: str | None = None,
                 pod: str | None = None, node_regex: str | None = None, pod_regex: str | None = None,
                 namespace: str | None = None, namespace_regex: str | None = None, duration: str = '5m'):
        if node and node_regex:
            raise ValueError(
                "At most one of node or node_regex can be defined")
        if pod and pod_regex:
            raise ValueError("At most one of pod or pod_regex can be defined")
        if namespace and namespace_regex:
            raise ValueError(
                "At most one of namespace or namespace_regex can be defined")
        # passed in parameters to filter queries
        self.node = node
        self.pod = pod
        self.namespace = namespace
        self.node_regex = node_regex
        self.pod_regex = pod_regex
        self.namespace_regex = namespace_regex
        self.filter_str = self.init_filter_str()

        self.duration = duration
        # csv files
        self._read_file = read_file
        self._progress_file = "csvs/query_progress.csv"
        self._save_file = "csvs/queried.csv"
        return

    # given a kubernetes component ('node', 'pod', 'namespace', etc.) and the name (optional)
    # of the component (e..g 'bp3d-worker-pod-a5343...') or a regex expression (optional)
    # for the component name,
    # return a string filtering for that component, e.g. 'pod="bp3d-worker-pod-a5343..."'
    def _get_component_filter_str(self, component: str, component_name: str = None, component_regex: str = None) -> str:
        if component_name is not None and component_regex is not None:
            raise ValueError(
                "at most one of component_name or component_regex should be defined.")
        # give a warning if it doesn't seem like the filter str is being used correctly
        known_k8s_components = ['node', 'pod', 'namespace', 'cluster', 'job',
                                'container', 'Hostname', 'UUID', 'device', 'endpoint', 'service']
        if component not in known_k8s_components:
            warnings.warn(
                f"Unknown component '{component}'. Known components are: {known_k8s_components}")
        # give the string depending on if it's a regex expression or not
        if component_name:
            return f'{component}="{component_name}'
        if component_regex:
            return f'{component}=~"{component_regex}"'
        # neither are defined
        return ""

    # update the filter string used for querying based on passed in filter parameters
    def update_filter_str(self, node=None, node_regex=None, pod=None, pod_regex=None, namespace=None, namespace_regex=None) -> str:
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
        for filter in filters:
            if filter == "":
                continue
            # in PromQL, it still works if the filter string ends in a comma
            filter_str += filter + ', '
        self.filter_str = filter_str
        return filter_str

    # initialize the filter string with the class's filters
    def init_filter_str(self):
        return self.update_filter_str(self.node, self.node_regex, self.pod, self.pod_regex, self.namespace, self.namespace_regex)

    def get_gpu_queries(self) -> dict[str:str]:
        # graph queries
        queries = {
            # total gpu usage = gpu_utilization% by pod but averaging out all the pods
            'total_gpu_usage': 'avg_over_time(namespace_gpu_utilization' + self.filter_str,
            'requested_gpus': 'count(DCGM_FI_DEV_GPU_TEMP{' + self.filter_str + '})'
        }
        queries = {}
        return queries

    def get_gpu_compute_resource_queries(self) -> dict[str, str]:
        # graph queries
        queries = {
            'gpu_utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'memory_copy_utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'fan_speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}'
        }
        return queries

    def get_rgw_queries(self) -> dict[str, str]:
        # graph queries
        queries = {
            'rgw_queue_length': 'sum by(instance) (ceph_rgw_qlen{' + self.filter_str + '})',
            'rgw_cache_hit': 'sum by(instance) (irate(ceph_rgw_cache_hit{' + self.filter_str + '}))',
            'rgw_cache_miss': 'sum by(instance) (irate(ceph_rgw_cache_miss{' + self.filter_str + '}))',
            'rgw_gets': 'sum by(instance) (irate(ceph_rgw_get{' + self.filter_str + '}))',
            'rgw_puts': 'sum by(instance) (irate(ceph_rgw_put{' + self.filter_str + '}))',
            'rgw_failed_req': 'sum by(instance) (irate(ceph_rgw_failed_req{' + self.filter_str + '}))'
        }

        return queries

    # queries from ../training_data_handling/work_flow.py
    def get_cpu_compute_resource_queries(self, start, duration_seconds: int | float) -> dict[str, str]:
        start = datetime_ify(start)
        duration_seconds = int(duration_seconds)
        offset = calculate_offset(start, duration_seconds)
        static_offset = calculate_offset(start, 10)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))

        # all resources and the heart of their queries
        queries = {
            # static metrics
            'cpu_request': 'sum by (pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{resource="cpu", ' + self.filter_str + '} offset ' + static_offset + ')',

            'mem_request': 'sum by (pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{resource="memory", ' + self.filter_str + '} offset ' + static_offset + ')',

            # non static metrics
            'mem_usage': 'sum by (pod) (max_over_time(container_memory_working_set_bytes{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',

            'cpu_usage': 'sum by (pod) (increase(container_cpu_usage_seconds_total{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',

            'transmitted_packets': 'sum by (pod) (increase(container_network_transmit_packets_total{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',

            'received_packets': 'sum by (pod) (increase(container_network_receive_packets_total{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',

            'transmitted_bandwidth': 'sum by (pod) (increase(container_network_transmit_bytes_total{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',

            'received_bandwidth': 'sum by (pod) (increase(container_network_receive_bytes_total{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))'
        }

        return queries

    def _get_queries(self, gpu_queries=False, gpu_compute_resource_queries=False, rgw_queries=False, cpu_compute_resource_queries=False):
        graph_queries = {}
        non_graph_queries = {}

        # TODO: figure out how to deal with sum_by
        # idea: sum_by never matters because we always want one datapoint for each cell
        # therefore, we should just sum everything, keeping sum_by as None.
        if gpu_queries:
            new_queries = self.get_gpu_queries()
            graph_queries.update(new_queries)
        if gpu_compute_resource_queries:
            new_queries = self.get_gpu_compute_resource_queries()
            graph_queries.update(new_queries)
        if rgw_queries:
            new_queries = self.get_rgw_queries()
            graph_queries.update(new_queries)

        if cpu_compute_resource_queries:
            new_queries = self.get_cpu_compute_resource_queries()
            non_graph_queries.update(new_queries)

        return {'graph': graph_queries, 'non_graph': non_graph_queries}

    # given a row of a dataframe, a graphs class instantiation, and a dict of queries for graphs,
    # return a queried version of that row,
    # with the new columns being the graph titles prepended with 'graph_'
    def _query_row_for_graphs(self, row: pd.Series, graphs_class: Graphs, graph_queries: dict[str, str]) -> pd.Series:
        # sum by none since we're eventually getting it all down to one data point --> no need to split it further
        graphs_dict = graphs_class.get_graphs_from_queries(
            queries_dict=graph_queries, sum_by=None, start=row['start'], end=row['end'])
        for title, data in graphs_dict.items():
            new_title = "graph_" + title
            row[new_title] = data
        return row

    def query_df(self, df: pd.DataFrame = None, batch_size: int = 5, gpu_queries=False, gpu_compute_resource_queries=False, rgw_queries=False, cpu_compute_resource_queries=False) -> pd.DataFrame:
        # handle user input
        if not (gpu_queries or gpu_compute_resource_queries or rgw_queries or cpu_compute_resource_queries):
            raise ValueError("No queries specified -> nothing to query")
        if df is None:
            try:
                pd.read_csv(self._read_file)
            except:
                raise ValueError(
                    f"No df passed in, and default read file ({self._read_file}) cannot be read.")

        queries_by_type = self._get_queries(
            gpu_queries=gpu_queries, gpu_compute_resource_queries=gpu_compute_resource_queries, rgw_queries=rgw_queries, cpu_compute_resource_queries=cpu_compute_resource_queries
        )
        # TODO: query in batches
        # print(f"Querying in batches of {batch_size} rows...")

        # query graphs
        graphs_class = Graphs()
        graph_queries = queries_by_type['graph']
        df = df.apply(self._query_row_for_graphs,
                      args=(graphs_class, graph_queries), axis=1)
        df.to_csv("csvs/graphs_queried.csv")
        # TODO: query normal rows (maybe with table.py?)
        # IDEA: instead of querying and then summing, try wrapping queries in sum() blocks?
        return df
