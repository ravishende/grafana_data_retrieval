# autopep8: off
from datetime import timedelta
import pandas as pd
import warnings
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
# Adjust the path to go up one level
sys.path.append("../../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.time_functions import calculate_offset, delta_to_time_str, datetime_ify
# autopep8: on


class Query_handler():
    def __init__(self, read_file="csvs/run_inputs.csv", node=None, pod=None, node_regex=None, pod_regex=None, namespace=None, namespace_regex=None, duration='1h'):
        # passed in parameters to filter queries
        if node and node_regex:
            raise ValueError(
                "At most one of node or node_regex can be defined")
        if pod and pod_regex:
            raise ValueError("At most one of pod or pod_regex can be defined")
        if namespace and namespace_regex:
            raise ValueError(
                "At most one of namespace or namespace_regex can be defined")

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
        known_k8s_components = ['node', 'pod', 'namespace', 'cluster']
        if component not in known_k8s_components:
            warnings.warn(
                f"Unknown component '{component}'. Known components are: {known_k8s_components}")
        # give the string depending on if it's a regex expression or not
        if component_name:
            return f'{component}="{component_name}'
        if component_regex:
            return f'{component}=~"{component_regex}"'
        # if neither are defined, query for it not being empty
        return f'{component}!=""'

    # get filter str using passed in values during init
    def init_filter_str(self) -> str:
        node_filter = self._get_component_filter_str(
            "node", self.node, self.node_regex)
        pod_filter = self._get_component_filter_str(
            "pod", self.pod, self.pod_regex)
        namespace_filter = self._get_component_filter_str(
            "namespace", self.namespace, self.namespace_regex)
        return f"{node_filter}, {pod_filter}, {namespace_filter}"

    def get_gpu_queries(self, start, duration_seconds: int | float) -> tuple[dict, list]:
        start = datetime_ify(start)
        duration_seconds = int(duration_seconds)
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))
        # graph queries
        graph_queries_no_sum_by = {
            # total gpu usage might not be a graph query
            'total_gpu_usage': 'avg_over_time(namespace_gpu_utilization{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')',
            'requested_gpus': 'count(DCGM_FI_DEV_GPU_TEMP{' + self.filter_str + '} offset ' + offset + ')'
        }
        # table queries
        table_queries_no_sum_by = {
            'gpu_utilization': 'sum(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')/count(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')',

            'gpus_requested': 'count(DCGM_FI_DEV_GPU_TEMP{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')'
        }
        queries = {}
        sum_by = None
        return queries, sum_by

    def get_gpu_compute_resource_queries(self, start, duration_seconds: int | float) -> tuple[dict, list]:
        start = datetime_ify(start)
        duration_seconds = int(duration_seconds)
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))
        # graph queries
        queries = {
            'gpu_utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'memory_copy_utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'fan_speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset
        }
        sum_by = ["namespace", "pod"]
        return queries, sum_by

    def get_rgw_queries(self, start, duration_seconds: int | float) -> tuple[dict, list]:
        start = datetime_ify(start)
        duration_seconds = int(duration_seconds)
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))
        # graph queries
        queries = {
            'queue_length': 'sum by(instance) (ceph_rgw_qlen{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')',
            'rgw_cache_hit': 'sum by(instance) (irate(ceph_rgw_cache_hit{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'rgw_cache_miss': 'sum by(instance) (irate(ceph_rgw_cache_miss{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'rgw_gets': 'sum by(instance) (irate(ceph_rgw_get{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'rgw_puts': 'sum by(instance) (irate(ceph_rgw_put{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'rgw_failed_req': 'sum by(instance) (irate(ceph_rgw_failed_req{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))'
        }
        sum_by = ["instance"]
        return queries, sum_by

    # queries from ../training_data_handling/work_flow.py
    def get_cpu_compute_resource_queries(self, start, duration_seconds: int | float) -> tuple[dict, list]:
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

        sum_by = ['pod']
        # assemble the final query
        return queries, sum_by

    def query_df(self, df: pd.DataFrame = None, batch_size: int = 5, gpu_queries=False, gpu_compute_resource_queries=False, rgw_queries=False, cpu_compute_resource_queries=False) -> pd.DataFrame:
        # handle user input
        if not (gpu_queries and gpu_compute_resource_queries and rgw_queries and cpu_compute_resource_queries):
            raise ValueError("No queries specified -> nothing to query")
        if df is None:
            try:
                pd.read_csv(self._read_file)
            except:
                raise ValueError(
                    f"no df passed in, and default read file ({self._read_file}) cannot be read.")

        # query in batches
        print(f"Querying in batches of {batch_size} rows...")
        # TODO: add logic for querying
        return df
