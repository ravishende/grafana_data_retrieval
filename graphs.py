import pandas as pd
from utils import query_api_site_for_graph, get_result_list, get_worker_id, get_time_dict_from_str
from inputs import NAMESPACE, DEFAULT_DURATION, DEFAULT_GRAPH_TIME_OFFSET, DEFAULT_GRAPH_STEP
from termcolor import colored
from datetime import datetime, timedelta
from tqdm import tqdm
import time


class Graphs():
    def __init__(self, namespace=NAMESPACE, end=datetime.now(), duration=DEFAULT_DURATION, time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP):
        # variables for querying data for graphs
        self.namespace = namespace
        self.end = end
        self.duration = duration
        self.time_offset = time_offset
        self.time_step = time_step

        # dict storing titles and their queries
        self.queries_dict = {
            'CPU Usage': 'sum by(node, pod) (node_namespace_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"})',
            'Memory Usage (w/o cache)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '", container!="", image!=""})',
            'Receive Bandwidth': 'sum by(node, pod) (irate(container_network_receive_bytes_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Transmit Bandwidth': 'sum by(node, pod) (irate(container_network_transmit_bytes_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Received Packets': 'sum by(node, pod) (irate(container_network_receive_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Transmitted Packets': 'sum by(node, pod) (irate(container_network_transmit_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Received Packets Dropped': 'sum by(node, pod) (irate(container_network_receive_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Transmitted Packets Dropped': 'sum by(node, pod) (irate(container_network_transmit_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
        }
        self.partial_queries_dict = {
            # These two graphs need 2 queries each to calculate them. It didn't work to get everything with one query
            'IOPS(Read+Write)': [
                'ceil(sum by(node, pod) (rate(container_fs_reads_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']) + ))',
                'ceil(sum by(node, pod) (rate(container_fs_writes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + '])))'
            ],
            'ThroughPut(Read+Write)': [
                'sum by(node, pod) (rate(container_fs_reads_bytes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
                'sum by(node, pod) (rate(container_fs_writes_bytes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
            ]
        }

    # given an end_time (datetime object) and an offset_str (string) (e.g. "12h5m30s"),
    #  return a new datetime object offset away from the end_time
    def _find_time_from_offset(self, end_time, offset_str):
        # get the offset in a usable form: {..., 'hours':____, 'minutes':___, 'seconds':____}
        time_dict = get_time_dict_from_str(offset_str)
        # create new datetime timedelta to represent the time offset and pass in parameters as values from time_dict
        time_offset = timedelta(**time_dict)
        # return the start time
        return end_time-time_offset

    # assembles string for the time filter to be passed into query_api_site_for_graph()
    def _assemble_time_filter(self):
        # calculate start time
        start = self._find_time_from_offset(self.end, self.time_offset)
        # assemble strings
        end_str = self.end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        # combine strings into time filter format
        time_filter = f'start={start_str}&end={end_str}&step={self.time_step}'

        return time_filter

