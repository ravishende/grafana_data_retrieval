import pandas as pd
from utils import query_api_site, get_result_list, filter_df_for_workers
from inputs import NAMESPACE, DEFAULT_DURATION


class Header():
    def __init__(self, namespace=NAMESPACE, duration=DEFAULT_DURATION):
        self.namespace = namespace
        self.duration = duration
        self.queries = {
            'CPU Utilisation (from requests)': 'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + self.namespace + '", resource="cpu"})',
            'CPU Utilisation (from limits)': 'sum by (node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}) / sum by(node, pod) (kube_pod_container_resource_limits{job="kube-state-metrics", namespace="' + self.namespace + '", resource="cpu"})',
            'Memory Utilisation (from requests)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '",container!="", image!=""}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + self.namespace + '", resource="memory"})',
            'Memory Utilisation (from limits)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '",container!="", image!=""}) / sum by(node, pod) (kube_pod_container_resource_limits{job="kube-state-metrics", namespace="' + self.namespace + '", resource="memory"})'
        }

    # returns a dataframe containing nodes, pods, and values for
    # a given json_data from a query (header data)
    def _generate_df(self, col_title, raw_json_data):
        # parse json data and initialize dataframe
        res_list = get_result_list(raw_json_data)
        df = pd.DataFrame(columns=['Node', 'Pod', col_title])

        # fill in dataframe
        for datapoint in res_list:
            # each triplet is a dictionary with node (str), pod (str), values (list)
            node = datapoint['metric']['node']
            pod = datapoint['metric']['pod']
            # timestamp = datapoint['value']['0']
            # multiply by 100 to get value in % form instead of decimal form.
            value = float(datapoint['value'][1])*100
            # add row to the end of the dataframe containing the node, pod, and value
            df.loc[len(df.index)] = [node, pod, value]
        return df

    # returns a dict in the form {header_title:dataframe}
    # where the dataframe contains header values per node, pod
    def get_header_dict(self, only_include_worker_pods=False):
        header_dict = {}

        # generate a dataframe for each header item, then add it to header_dict
        for query_title, query in self.queries.items():
            # generate dataframe
            json_data = query_api_site(query)
            header_item = self._generate_df(query_title, json_data)

            # filter by worker pods if requested
            if only_include_worker_pods:
                header_item = filter_df_for_workers(header_item)

            header_dict[query_title] = header_item

        return header_dict
