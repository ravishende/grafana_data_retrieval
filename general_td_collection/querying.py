import pandas as pd


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

    def get_gpu_queries(self):
        # graph queries
        graph_queries_no_sum_by = 'sum(cDCGM_FI_DEV_GPU_UTIL{' + self.filter_str + \
            '})/count(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '})'

        # table queries
        table_queries_no_sum_by = {
            'GPU Utilization': 'sum(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '}) / count(DCGM_FI_DEV_GPU_UTIL{' + self.filter_str + '})',
            'GPUs Requested': 'count(DCGM_FI_DEV_GPU_TEMP{' + self.filter_str + '})'
        }
        queries = {}
        sum_by = ["pod"]
        return queries, sum_by

    def get_gpu_compute_resource_queries(self):
        queries = {
            'GPU Utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'Memory Copy Utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'Power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'Temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}',
            'Fan Speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{' + self.filter_str + '}'
        }
        sum_by = ["namespace", "pod"]
        return queries, sum_by

    def get_rgw_queries(self):
        queries = {
            'Queue length': 'sum by(instance) (ceph_rgw_qlen{' + self.filter_str + '})',
            'RGW Cache Hit': 'sum by(instance) (irate(ceph_rgw_cache_hit{' + self.filter_str + '}[' + self.duration + ']))',
            'RGW Cache Miss': 'sum by(instance) (irate(ceph_rgw_cache_miss{' + self.filter_str + '}[' + self.duration + ']))',
            'RGW gets': 'sum by(instance) (irate(ceph_rgw_get{' + self.filter_str + '}[' + self.duration + ']))',
            'RGW puts': 'sum by(instance) (irate(ceph_rgw_put{' + self.filter_str + '}[' + self.duration + ']))',
            'RGW Failed Req': 'sum by(instance) (irate(ceph_rgw_failed_req{' + self.filter_str + '}[' + self.duration + ']))'
        }
        sum_by = ["instance"]
        return queries, sum_by

    # queries from ../training_data_handling/work_flow.py
    def get_cpu_compute_resource_queries(self):
        queries = {}
        sum_by = []
        return queries, sum_by
