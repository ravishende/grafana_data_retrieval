#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
from tqdm import tqdm
from helpers.querying import query_data
from helpers.filtering import filter_df_for_workers
from inputs import NAMESPACE


class Header():
    def __init__(self, namespace: str = NAMESPACE) -> None:
        self.namespace = namespace
        self.queries = {
            'CPU Utilisation (from requests)': 'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + self.namespace + '", resource="cpu"})',
            'CPU Utilisation (from limits)': 'sum by (node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}) / sum by(node, pod) (kube_pod_container_resource_limits{job="kube-state-metrics", namespace="' + self.namespace + '", resource="cpu"})',
            'Memory Utilisation (from requests)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '",container!="", image!=""}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + self.namespace + '", resource="memory"})',
            'Memory Utilisation (from limits)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '",container!="", image!=""}) / sum by(node, pod) (kube_pod_container_resource_limits{job="kube-state-metrics", namespace="' + self.namespace + '", resource="memory"})'
        }

    # returns a dataframe containing nodes, pods, and values for
    # a given result_list from a query (header data)
    def _generate_df(self, col_title: str, res_list: list[dict]) -> pd.DataFrame:
        # parse json data and initialize dataframe
        df = pd.DataFrame(columns=['Node', 'Pod', col_title])
        # df = pd.DataFrame(columns=['Time', 'Node', 'Pod', col_title])  # for timestamp

        # fill in dataframe
        for datapoint in res_list:
            # each triplet is a dictionary with node (str), pod (str), values (list)
            node = datapoint['metric']['node']
            pod = datapoint['metric']['pod']
            # timestamp = datapoint['value']['0']  # for timestamp

            # multiply by 100 to get value in % form instead of decimal form.
            value = float(datapoint['value'][1])*100

            # add row to the end of the dataframe containing the node, pod, and value
            df.loc[len(df.index)] = [node, pod, value]
            # df.loc[len(df.index)] = [timestamp, node, pod, value]  # for timestamp

        return df

    # returns a dict in the form {header_title:dataframe}
    # where the dataframe contains header values per node, pod
    def get_header_dict(self, only_include_worker_pods: bool = False) -> dict[str, pd.DataFrame]:
        header_dict = {}

        # generate a dataframe for each header item, then add it to header_dict
        for query_title, query in tqdm(self.queries.items()):
            # generate dataframe
            result_list = query_data(query)
            header_item = self._generate_df(query_title, result_list)

            # filter by worker pods if requested
            if only_include_worker_pods:
                header_item = filter_df_for_workers(header_item)

            header_dict[query_title] = header_item

        return header_dict
