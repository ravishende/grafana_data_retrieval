#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
from utils import query_api_site_for_graph, get_result_list, get_time_dict_from_str, filter_df_for_workers
from inputs import NAMESPACE, DEFAULT_DURATION, DEFAULT_GRAPH_TIME_OFFSET, DEFAULT_GRAPH_STEP
from termcolor import colored
from datetime import datetime, timedelta
from pprint import pprint
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
            # These two graphs need 2 queries each to calculate them.
            # It didn't work to get everything with one query
            'IOPS(Read+Write)': [
                'ceil(sum by(node, pod) (rate(container_fs_reads_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']) + ))',
                'ceil(sum by(node, pod) (rate(container_fs_writes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + '])))'
            ],
            'ThroughPut(Read+Write)': [
                'sum by(node, pod) (rate(container_fs_reads_bytes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
                'sum by(node, pod) (rate(container_fs_writes_bytes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
            ]
        }

    # given an end_time (datetime) and an offset_str (string) (e.g. "12h5m30s"),
    # return a new datetime object offset away from the end_time
    def _find_time_from_offset(self, end_time, offset_str):
        # get the offset in a usable form: {..., 'hours':____, 'minutes':___, 'seconds':____}
        time_dict = get_time_dict_from_str(offset_str)
        # create new datetime timedelta to represent the time
        # offset and pass in parameters as values from time_dict
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

            # store the two queries' values. Originally graph_df only stores read
            # values instead of read+write. Later, it is updated to store both.
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

    # returns a dict in the form: {'dropped': {pod: index_dropped, ...}, 'recovered':{pod: index_recovered, ...} }
    # returns none if no losses
    def _check_graph_loss(self, graph_title, graph_df, print_info=False):
        #variables
        previous_value = 0
        previous_pod = None
        #data to return
        # graph_losses = {
        #     'drop_prev_values': [],
        #     'back_online_values': [],
        #     'pods_dropped':[],
        #     'pods_back_online':[]
        # }
        pods_dropped_indeces = {}
        pods_recovered_indeces = {}
        #loop through looking for lost and/or recovered pods
        for index in range(len(graph_df)):
            # store new pod and value
            current_value = graph_df[graph_title][index]
            current_pod = graph_df["Pod"][index]

            # when switching between pods, it doesn't matter if values change
            if previous_pod != current_pod:
                previous_value = current_value
                previous_pod = current_pod
                continue

            # pod dropped
            if previous_value != 0 and current_value == 0:
                pods_dropped_indeces[current_pod] = index
                # graph_losses[drop_prev_values].append(previous_value)
                # graph_losses[pods_dropped].append(current_pod)


            # pod recovered
            if previous_value == 0 and current_value != 0:
                if current_pod in pods_dropped_indeces.keys():
                    pods_recovered_indeces[current_pod] = index
                    # back_online_values.append(graph_losses[current_value])
                    # pods_back_online.append(graph_losses[current_pod])

            #update old pod and value for next iteration
            previous_value = current_value
            previous_pod = current_pod

        # cannot have recoverd pods if none were dropped.
        # if both are dropped and recovered are empty, return None
        if len(pods_dropped_indeces) == 0:
            return None

        # print collected statistics
        if print_info:
            # Print Info for Pods Dropped: pod, index dropped, previous value
            print("\n\n" + '='*30)
            print(colored(graph_title, "blue"), "\n" + '='*30)
            print(colored("Pods Dropped || Index || Previous Value", "green"))
            for pod, ind in pods_dropped_indeces.items():
                prev_val = graph_df[graph_title][ind-1]
                print(f'{pod} || {ind} || {prev_val}')

            # Print Info for Pods Recovered: pod, index recovered, recovered value
            print("\n")
            print(colored("Pods Recovered || Index || Recovered Value", "green"))
            for pod, ind in pods_recovered_indeces.items():
                recovered_val = graph_df[graph_title][ind]
                print(f'{pod} || {ind} || {recovered_val}')


        return {'dropped': pods_dropped_indeces, 'recovered': pods_recovered_indeces}

       

    def check_for_losses(self, graphs_dict=None, print_info=False):
        # generate graphs_dict if it isn't passed in
        if graphs_dict is None:
            graphs_dict = self.get_graphs_dict()

        graphs_losses_dict = {}

        print("*"*100)
        print(colored("Check For Dropped Pods:", "magenta"))
        print("*"*100)

        for graph_title, graph in graphs_dict.items():
            #collect and store loss data
            graph_loss_data = self._check_graph_loss(graph_title, graph, print_info=print_info)
            graphs_losses_dict[graph_title] = graph_loss_data

        return graphs_losses_dict

    def _requery_graph_loss(self, graph, graph_loss_dict):
        graph_losses = {'dropped': [], 'recovered': []}
        # for each drop index


        # for category, losses in graph_loss_dict:
            # look up graph timestamp for drop index and the prior index
            #check if timestamp is in seconds or not, then run the assemble_time_stamp on it
            # requery with those two timestamps as end_time and start_time respectively with graph_step *10
            # append to graph_loss['dropped'] 
        # repeat process for each recovery index, appending to graph_loss['recovered'] instead
        return graph_losses

    def requery_losses(self, graphs_dict):
        requeried_graphs_dict = {}
        # example structure: 
        '''
        requeried_graphs_dict = {
            'Received Bandwidth':{
                'dropped':[graph_df_1, graph_df_2, graph_df_3], 
                'recovered':[graph_df_1, graph_df_2]
            }, 
            'Transmit Bandwidth':{
                'dropped':[...], 
                'recovered':[...]
            } 
        }
        '''
        # code:
        # for each graph in graphs_losses
        return requeried_graphs_dict

    def print_requeried_graphs(self, requeried_graphs_dict=None):
        if requeried_graphs_dict is None:
            requeried_graphs_dict = self.requery_losses()

