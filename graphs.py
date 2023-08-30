import pandas as pd
from utils import query_api_site_for_graph, get_result_list, get_time_dict_from_str, filter_df_for_workers
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

    # given an end_time (datetime object) and an offset_str (string) (e.g. "12h5m30s"), return a new datetime object offset away from the end_time
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

    # get 4 lists: times, values, nodes, and pods for a given graph query
    def _generate_graph_df(self, query_title, query, show_runtimes=False):
        # create time filter to then generate list of all datapoints for the graph
        time_filter = self._assemble_time_filter()

        if show_runtimes:
            start = time.time()

        result_list = get_result_list(query_api_site_for_graph(query, time_filter))

        if show_runtimes:
            end = time.time()
            print("\ntime elapsed for querying:", colored(end-start, "green"))

        times_column = []
        values_column = []
        pods_column = []
        nodes_column = []

        # loop through the data for each pod. The data has: node, pod, values, timestamps
        for datapoint in result_list:
            # prepare data to be extracted
            values_list = []
            times_list = []

            # fill in lists
            for time_value_pair in datapoint['values']:
                times_list.append(time_value_pair[0])
                values_list.append(float(time_value_pair[1]))
            # There is only one node and pod per pod, so these columns will be constant for each pod.
            node_list = [datapoint['metric']['node']]*len(times_list)
            pod_list = [datapoint['metric']['pod']]*len(times_list)

            # add pod's lists to the whole graph's lists
            times_column.extend(times_list)
            values_column.extend(values_list)
            nodes_column.extend(node_list)
            pods_column.extend(pod_list)

        # make and populate graph dataframe
        graph_df = pd.DataFrame()
        graph_df['Time'] = times_column
        graph_df['Node'] = nodes_column
        graph_df['Pod'] = pods_column
        graph_df[query_title] = values_column

        return graph_df

    # get a dictionary in the form of {graph titles: list of graph data}
    def _generate_graphs(self, show_runtimes=False):
        graphs_dict = {}
        # get all of the initial graphs from the normal queries
        for query_title, query in tqdm(self.queries_dict.items()):
            if show_runtimes:
                start_time = time.time()

            # collect graph data
            graph_df = self._generate_graph_df(query_title, query, show_runtimes=show_runtimes)
            graphs_dict[query_title] = graph_df

            if show_runtimes:
                end_time = time.time()
                print("total time elapsed:", colored(end_time-start_time, "green"), "\n\n")

        # get graphs from partial queries
        for query_title, query_pair in tqdm(self.partial_queries_dict.items()):
            if show_runtimes:
                start_time = time.time()

            # store the two queries' values. Originally graph_df only stores read values instead of read+write. Later, it is updated to store both.
            graph_df = self._generate_graph_df(query_title, query_pair[0], show_runtimes=show_runtimes)
            graph_df_write = self._generate_graph_df(query_title, query_pair[1], show_runtimes=show_runtimes)

            # calculate read + write column by adding read values and write values
            graph_df[query_title] = graph_df[query_title] + graph_df_write[query_title]
            # add graph dataframe to graphs_dict
            graphs_dict[query_title] = graph_df

            if show_runtimes:
                end_time = time.time()
                print("total time elapsed:", colored(end_time-start_time, "green"), "\n\n")

        return graphs_dict

    # generate and return a list of all the graphs
    def get_graphs_dict(self, only_include_worker_pods=False, display_time_as_timestamp=True, show_runtimes=False):
        graphs_dict = self._generate_graphs(show_runtimes=show_runtimes)
        for graph_title, graph in graphs_dict.items():
            # for every worker pod in graph, change pod's value to just be the worker id, drop all non-worker pods
            if only_include_worker_pods:
                graph = filter_df_for_workers(graph)

            # update graphs with correct time columns
            if display_time_as_timestamp:
                graph['Time'] = pd.to_datetime(graph['Time'], unit="s")

            graphs_dict[graph_title] = graph

        return graphs_dict
