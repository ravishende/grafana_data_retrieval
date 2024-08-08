# autopep8: off
from datetime import timedelta
import pandas as pd
import sys
import os
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
import sys
import os
# Adjust the path to go up one level
sys.path.append("../../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.time_functions import calculate_offset, delta_to_time_str
# autopep8: on


def query_df(df):
    return df


class Query_handler():
    def __init__(self, node=None, pod=None, node_regex=None, pod_regex=None, namespace=None, namespace_regex=None, duration='1h'):
        # passed in parameters to filter queries
        if node and node_regex:
            raise ValueError(
                "At most one of node or node_regex can be defined")
        if pod and pod_regex:
            raise ValueError("At most one of pod or pod_regex can be defined")
        if namespace and namespace_regex:
            raise ValueError(
                "At most one of namespace or namespace_regex can be defined")

        self.duration = duration
        self.node = node
        self.pod = pod
        self.namespace = namespace
        self.node_regex = node_regex
        self.pod_regex = pod_regex
        self.namespace_regex = namespace_regex
        self.filter_str = self.init_filter_str()
        return

    # TODO: GET BETTER NAME THAN thing AND thing_name. THEN RENAME FUNCTION
    def _get_component_filter_str(self, component, component_name, component_regex):
        if component_name:
            return f'{component}="{component_name}'
        if component_regex:
            return f'{component}=~"{component_regex}"'
        # if neither are defined, query for it not being empty
        return f'{component}!=""'

    def init_filter_str(self):
        node_filter = self._get_component_filter_str(
            "node", self.node, self.node_regex)
        pod_filter = self._get_component_filter_str(
            "pod", self.pod, self.pod_regex)
        namespace_filter = self._get_component_filter_str(
            "namespace", self.namespace, self.namespace_regex)
        return f"{node_filter}, {pod_filter}, {namespace_filter}"

    def get_gpu_queries(self, start, duration_seconds):
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))
        # graph queries
        graph_queries_no_sum_by = {
            # total gpu usage might not be a graph query
            'Total GPU usage': 'avg_over_time(namespace_gpu_utilization{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')',
            'Requested GPUs': 'count(DCGM_FI_DEV_GPU_TEMP{' + self.filter_str + '} offset ' + offset + ')'
        }
        # table queries
        table_queries_no_sum_by = {
            'GPU Utilization': 'sum(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')/count(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')',

            'GPUs Requested': 'count(DCGM_FI_DEV_GPU_TEMP{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')'
        }
        queries = {}
        sum_by = None
        return queries, sum_by

    def get_gpu_compute_resource_queries(self, start, duration_seconds):
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))
        # graph queries
        queries = {
            'GPU Utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'Memory Copy Utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'Power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'Temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset,
            'Fan Speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}[' + duration + '] offset ' + offset
        }
        sum_by = ["namespace", "pod"]
        return queries, sum_by

    def get_rgw_queries(self, start, duration_seconds):
        offset = calculate_offset(start, duration_seconds)
        duration = delta_to_time_str(timedelta(seconds=duration_seconds))
        # graph queries
        queries = {
            'Queue length': 'sum by(instance) (ceph_rgw_qlen{' + self.filter_str + '}[' + duration + '] offset ' + offset + ')',
            'RGW Cache Hit': 'sum by(instance) (irate(ceph_rgw_cache_hit{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'RGW Cache Miss': 'sum by(instance) (irate(ceph_rgw_cache_miss{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'RGW gets': 'sum by(instance) (irate(ceph_rgw_get{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'RGW puts': 'sum by(instance) (irate(ceph_rgw_put{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))',
            'RGW Failed Req': 'sum by(instance) (irate(ceph_rgw_failed_req{' + self.filter_str + '}[' + duration + '] offset ' + offset + '))'
        }
        sum_by = ["instance"]
        return queries, sum_by

    # queries from ../training_data_handling/work_flow.py
    def get_cpu_compute_resource_queries(self, start, duration_seconds):
        # get all the pieces necessary to assemble the queries
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
