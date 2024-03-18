#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
from helpers.querying import query_data_for_graph
from helpers.printing import print_sub_title
from helpers.filtering import filter_df_for_workers
from helpers.time_functions import datetime_ify, delta_to_time_str, time_str_to_delta, find_time_from_offset
from inputs import *
from datetime import datetime, timedelta
from termcolor import colored
from tqdm import tqdm
import time
import re


class Graphs():
    def __init__(self, namespace=NAMESPACE, end=DEFAULT_FINAL_GRAPH_TIME, duration=DEFAULT_DURATION, time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP, requery_step_divisor=REQUERY_GRAPH_STEP_DIVISOR):
        # variables for querying data for graphs
        self.namespace = namespace
        self.end = end
        self.duration = duration
        self.time_offset = time_offset
        self.time_step = time_step
        self.requery_step_divisor = requery_step_divisor

        # dict storing titles and their queries.
        self.queries = {  # Note: Do not change the white space in 'sum by(node, pod) ' because _update_query_for_requery() relies on it
            'CPU Usage': 'sum by(node, pod) (irate(node_namespace_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Memory Usage (w/o cache)': 'sum by(node, pod) (irate(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '", container!="", image!=""}[' + self.duration + ']))',
            'Receive Bandwidth': 'sum by(node, pod) (irate(container_network_receive_bytes_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Transmit Bandwidth': 'sum by(node, pod) (irate(container_network_transmit_bytes_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Received Packets': 'sum by(node, pod) (irate(container_network_receive_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Transmitted Packets': 'sum by(node, pod) (irate(container_network_transmit_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Received Packets Dropped': 'sum by(node, pod) (irate(container_network_receive_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
            'Rate of Transmitted Packets Dropped': 'sum by(node, pod) (irate(container_network_transmit_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
        }
        self.partial_queries = {
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

    # assembles string for the time filter to be passed into query_data_for_graph()
    def _assemble_time_filter(self, start=None, end=None, time_step=None):
        # set default values if not given
        if time_step is None:
            time_step = self.time_step
        if start is None and end is None:
            # calculate start time
            start = find_time_from_offset(end=self.end, offset=self.time_offset)
            end = self.end
        elif start is None or end is None:  # one is none but not both
            raise ValueError("start and end must either be both defined or both None")
        else:
            # make sure both start and end are datetime objects
            start = datetime_ify(start)
            end = datetime_ify(end)

        # assemble strings
        end_str = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        # combine strings into time filter format
        time_filter = f'start={start_str}&end={end_str}&step={time_step}'

        return time_filter

    # returns a dataframe containing Time, node, pod, and value (value is titled something different for each graph)
    # returns none if there is no data
    # note: sum_by is a string or list of strings that must have the same items that queries start with in their "sum by(___, ___) (...)"
            # this is only used by get_graphs_from_queries
    def _generate_graph_df(self, query_title, query, start=None, end=None, time_step=None, sum_by=["node", "pod"], show_runtimes=False):
        # create time filter to then generate list of all datapoints for the graph
        time_filter = self._assemble_time_filter(start=start, end=end, time_step=time_step)

        if show_runtimes:
            runtime_start = time.time()

        # query for data
        result_list = query_data_for_graph(query, time_filter)
        if len(result_list) == 0:
            return None

        if show_runtimes:
            runtime_end = time.time()
            print("\ntime elapsed for querying:", colored(runtime_end-runtime_start, "green"))

        # prepare columns for graph. columns will be appended to one pod at a time
        times_column = []
        values_column = []
        
        # initialize sum_by columns to eventually be put into the graph
        if sum_by is not None: # if sum_by is None, don't add a sum_by column
            sum_by_columns = {metric:[] for metric in sum_by}
        
        # loop through the data for each pod. The data has: node, pod, values, timestamps
        for datapoint in result_list:
            # prepare data to be extracted
            values_list = []
            times_list = []

            # fill in lists
            for time_value_pair in datapoint['values']:
                times_list.append(time_value_pair[0])
                values_list.append(float(time_value_pair[1]))
            
            # add pod's lists to the graph's columns
            times_column.extend(times_list)
            values_column.extend(values_list)

            if sum_by is not None:
                for item in sum_by:
                    item_list = [datapoint['metric'][item]]*len(times_list)
                    sum_by_columns[item].extend(item_list)
           
        # create and populate graph dataframe
        graph_df = pd.DataFrame()
        graph_df['Time'] = times_column
        # add in sum_by columns
        if sum_by is not None:  # if sum_by is None, don't add a sum_by column
            for item in sum_by:
                # make sure item cols in graph are title case (Capitalized First Letters Of Words)
                graph_df[item.title()] = sum_by_columns[item]
        # add in values column
        graph_df[query_title] = values_column

        return graph_df

    # get a dictionary in the form of {graph titles: list of graph data}
    def _generate_graphs(self, show_runtimes=False):
        queries_dict = self.queries
        partial_queries_dict = self.partial_queries
        graphs_dict = {}

        # get all of the initial graphs from the normal queries
        for query_title, query in tqdm(queries_dict.items()):
            if show_runtimes:
                start_time = time.time()

            # collect graph data
            graph_df = self._generate_graph_df(query_title, query, show_runtimes=show_runtimes)
            graphs_dict[query_title] = graph_df

            if show_runtimes:
                end_time = time.time()
                print("total time elapsed:", colored(end_time-start_time, "green"), "\n\n")

        # get graphs from partial queries
        for query_title, query_pair in tqdm(partial_queries_dict.items()):
            if show_runtimes:
                start_time = time.time()

            # store the two queries' values. Originally graph_df only stores read
            # values instead of read+write. Later, it is updated to store both.
            graph_df = self._generate_graph_df(query_title, query_pair[0], show_runtimes=show_runtimes)
            graph_df_write = self._generate_graph_df(query_title, query_pair[1], show_runtimes=show_runtimes)

            if graph_df is not None and graph_df_write is not None:
                # calculate read + write column by adding read values and write values
                graph_df[query_title] = graph_df[query_title] + graph_df_write[query_title]

            # add graph dataframe to graphs_dict
            graphs_dict[query_title] = graph_df

            if show_runtimes:
                end_time = time.time()
                print("total time elapsed:", colored(end_time-start_time, "green"), "\n\n")

        return graphs_dict


    '''
    ============================
            User Methods
    ============================
    '''

    # Given a dictionary of queries, generate graphs based on those queries
    # Note: if the queries do not start with "sum by(Node, pod)", then you must set sum_by
    #       to be what the query has in "sum by(_____)""
    #       This function does not work if queries do not start with "sum by("
    # Return a dictionary of graphs of the same names as the queries
    def get_graphs_from_queries(self, queries_dict, sum_by=["node", "pod"], display_time_as_datetime=False):
        # handle if sum_by is a single input (put into list format)
        if isinstance(sum_by, str):
            sum_by = [sum_by]
        # make sure sum_by metrics are all lower case
        for i in range(len(sum_by)):
            sum_by[i] = sum_by[i].lower()
        
        # generate graphs
        graphs_dict={}
        for title, query in tqdm(queries_dict.items()):
            graphs_dict[title] = self._generate_graph_df(title, query, sum_by=sum_by)

        # update times to be datetimes if requested
        if display_time_as_datetime:
            for graph_title, graph in graphs_dict.items():
                if graph is None:
                    continue
                # update graphs with correct time columns
                graph['Time'] = pd.to_datetime(graph['Time'], unit="s")
                graphs_dict[graph_title] = graph

        return graphs_dict


    # generate and return a dictionary of all the graphs
    def get_graphs_dict(self, only_include_worker_pods=False, display_time_as_datetime=True, show_runtimes=False):
        graphs_dict = self._generate_graphs(show_runtimes=show_runtimes)
        # loop through graphs
        for graph_title, graph in graphs_dict.items():
            if graph is None:
                continue

            # for every worker pod in graph, change pod's value to just be the worker id, drop all non-worker pods
            if only_include_worker_pods:
                graph = filter_df_for_workers(graph)

            # update graphs with correct time columns
            if display_time_as_datetime:
                graph['Time'] = pd.to_datetime(graph['Time'], unit="s")

            graphs_dict[graph_title] = graph

        return graphs_dict


    # combines all graph dataframes into one large dataframe. Each graph is represented as a column
    # this works because all graphs are queried for the same time frame and time step. They also have the same pods set
    def get_graphs_as_one_df(self, graphs_dict=None, sum_by=['node', 'pod']):
        total_df = pd.DataFrame(data={})

        # Handle invalid input
        if graphs_dict is None and sum_by != ['node', 'pod']:
            raise ValueError("Must pass in graphs_dict if sum_by is not default value.")

        # Generate graphs if none given
        if graphs_dict is None:
            graphs_dict = self._generate_graphs()

        # Fill in Time and sum_by (default: node, pod) columns with the first non-empty graph
        for graph_df in graphs_dict.values():
            if graph_df is not None:
                # Insert Time column
                total_df['Time'] = graph_df['Time']
                # Insert sum_by columns
                if sum_by is not None:
                    # Handle if sum_by is single string
                    if isinstance(sum_by, str):
                        sum_by = [sum_by]
                    # Add in sum_by columns to total_df
                    for metric in sum_by:
                        metric_col = metric.title()
                        total_df[metric_col] = graph_df[metric_col]
                # once columns are inserted, break - start filling in df with graphs
                break

        # Fill in graphs columns
        for title, graph_df in graphs_dict.items():
            if graph_df is None:
                total_df[title] = None
                continue
            total_df[title] = graph_df[title]

        return total_df


    '''
    =========================================
              Requery Methods
    =========================================
    '''

    # convert a dataframe containing all graphs data into a dictionary with several graphs
    # used when a graphs_df is passed in instead of a graphs_dict in check_for_losses
    # this can happen when a user requests the graph data to be a single df, then passes that df back into check_for_losses
    def convert_graphs_df_to_dict(self, graphs_df):
        if not isinstance(graphs_df, pd.DataFrame):
            raise TypeError("Input must be a pandas DataFrame")
            return
        graphs_dict = {}
        for col_title in graphs_df.columns:
            if col_title == 'Node' or col_title == 'Pod' or col_title == 'Time':
                pass

            # assemble new df
            graph_data = {
                'Time': graphs_df['Time'],
                'Node': graphs_df['Node'],
                'Pod': graphs_df['Pod'],
                col_title: graphs_df[col_title]
            }
            graph_df = pd.DataFrame(data=graph_data)
            graphs_dict[col_title] = graph_df

        return graphs_dict

    # change a query to only query for the given pod
    def _update_query_for_requery(self, query, pod):
        # add specific pod to query so only the one specific pod is queried instead of all pods
        # insert the pod specification just before the namespace is specified
        namespace_index = query.find('namespace="')
        pod_str = f'pod="{pod}", '
        updated_query = query[:namespace_index] + pod_str + query[namespace_index:]

        return updated_query

    # returns a dictionary containing potential pod drops and recoveries in the form:
    # {'dropped': [{'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {...}, ...],
    #  'recovered': [{'pod':str, 'start':datetime, 'end':datetime, 'val':float}, {...}, ...]}
    # returns none if no losses
    def _check_graph_loss(self, graph_title, graph_df, print_info=False):
        previous_value = 0
        previous_pod = None
        
        # data to return
        pods_dropped = []
        pods_recovered = []

        # loop through looking for lost and/or recovered pods
        for index in range(len(graph_df)):
            # store new pod and value
            current_value = graph_df[graph_title][index]
            current_pod = graph_df["Pod"][index]

            # when switching between pods, it doesn't matter if values change
            if previous_pod != current_pod:
                previous_value = current_value
                previous_pod = current_pod
                continue

            # pod dropped: was nonzero, now is zero
            if previous_value != 0 and current_value == 0:
                drop = {
                    'pod': current_pod,
                    'start': graph_df['Time'][index-1],
                    'end': graph_df['Time'][index],
                    'prev val': previous_value
                }
                pods_dropped.append(drop)

            # pod recovered: was zero, now is nonzero
            if previous_value == 0 and current_value != 0:
                # check that pod was dropped in order for it to be recovered
                if any(pod_dict['pod'] == current_pod for pod_dict in pods_dropped):
                    recovery = {
                        'pod': current_pod,
                        'start': graph_df['Time'][index-1],
                        'end': graph_df['Time'][index],
                        'val': current_value
                    }
                    pods_recovered.append(recovery)

            # update previous pod and value for next iteration
            previous_value = current_value
            previous_pod = current_pod

        # once pod loss info is collected, check if there are any drops
        # cannot have recoverd pods if none were dropped.
        # if both dropped and recovered are empty, return None
        if len(pods_dropped) == 0:
            return None

        # print collected statistics
        if print_info and len(pods_dropped) > 0:
            print_sub_title(graph_title)
            # Print Info for Pods Dropped: pod, previous value, time of last value, time dropped,
            print(colored("Pods Dropped || Previous Value || Time of Previous Value || Time Dropped", "green"))
            for pod in pods_dropped:
                print(f'{pod["pod"]} || {round(pod["prev val"],3)} || {pod["start"]} || {pod["end"]}')

            if len(pods_recovered) > 0:
                # Print Info for Pods Recovered: pod, value, time of last 0, time recovered
                print(colored("\nPods Recovered || Recovered Value || Time of Previous 0 || Time Recovered", "green"))
                for pod in pods_recovered:
                    print(f'{pod["pod"]} || {round(pod["val"],3)} || {pod["start"]} || {pod["end"]}')

        return {'dropped': pods_dropped, 'recovered': pods_recovered}

    # returns a dictionary containing all potential dropped or recovered pods with information about them
    # the returned dictionary (graphs_losses_dict) is in the form
        # graphs_losses = {
        #     graph_title_1: {
        #         'dropped': [{'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {...}, ...],
        #         'recovered': [{'pod':str, 'start':datetime, 'end':datetime, 'val':float}, {...}, ...]
        #     },
        #     graph_title_2: {
        #         'dropped': [{'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {...}, ...],
        #         'recovered': [{'pod':str, 'start':datetime, 'end':datetime, 'val':float}, {...}, ...]
        #     },
        #     ...
        # }
    def check_for_losses(self, graphs_dict=None, print_info=False):
        # generate graphs_dict if it isn't passed in
        if graphs_dict is None:
            graphs_dict = self.get_graphs_dict()

        # check for if graphs_dict was input as single dataframe. If it is, convert it to a dict of graphs
        if isinstance(graphs_dict, pd.DataFrame):
            graphs_dict = self.convert_graphs_df_to_dict(graphs_dict)

        # check if graphs_dict has data
        elif all(value is None for value in graphs_dict.values()):
            raise ValueError("graphs_dict has no data; can't check for losses of no data")
            return
        
        graphs_losses_dict = {}
        # fill in graphs_losses_dict to be returned
        for graph_title, graph in graphs_dict.items():
            if graph is None:
                continue

            # collect and store loss data
            graph_loss_data = self._check_graph_loss(graph_title, graph, print_info=print_info)
            graphs_losses_dict[graph_title] = graph_loss_data

        return graphs_losses_dict

    # returns a dictionary of graph titles with each title containing two categories: dropped and retrieved
    # in each category, there is a list of graph dataframes. Each df corresponds to a pod that was dropped/recovered
    # the returned dictionary (requiered_graph_dict) is in the form
        # graphs_losses = {
        #     graph_title_1: {
        #         'dropped': [{'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {...}, ...],
        #         'recovered': [{'pod':str, 'start':datetime, 'end':datetime, 'val':float}, {...}, ...]
        #     },
        #     graph_title_2: {
        #         'dropped': [{'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {...}, ...],
        #         'recovered': [{'pod':str, 'start':datetime, 'end':datetime, 'val':float}, {...}, ...]
        #     },
        #     ...
        # }
    def requery_graphs(self, graphs_losses_dict, show_runtimes=False):
        # declare variables
        requeried_graphs_dict = {}
        query = ''
        query_pair = []
        partial_query = False

        # get graph titles and lable_dict (label_dict looks like {'dropped':[{pod},...], 'retrieved:[{pod},...]'})
        for graph_title, label_dict in tqdm(graphs_losses_dict.items()):
            if label_dict is None:
                continue
            requeried_graphs_dict[graph_title] = {}
            # get category (dropped or retrieved) and list of pod_dicts (containing pod, start, end (also 'val' or 'prev val' which we don't use here))
            for category, pods_list in label_dict.items():
                requeried_graphs_dict[graph_title][category] = []
                # get query (not updated for specific pod)
                if graph_title in self.queries.keys():
                    query = self.queries[graph_title]
                    partial_query = False
                else:
                    # partial query --> 2 queries per graph
                    query_pair = self.partial_queries[graph_title]
                    partial_query = True

                # loop through the pods for each graph
                for pod_dict in pods_list:
                    # assemble arguments for self._generate_graph_df
                    pod = pod_dict['pod']
                    start = pod_dict['start']
                    end = pod_dict['end']
                    # convert time_step to timedelta to be able to divide it
                    time_step = time_str_to_delta(self.time_step)/self.requery_step_divisor
                    # convert time_step back to str to be used for querying
                    time_step = delta_to_time_str(time_step)
                    graph_df = None

                    # graph is defined by 1 query --> query graph
                    if not partial_query:
                        updated_query = self._update_query_for_requery(query, pod)
                        graph_df = self._generate_graph_df(
                            graph_title, updated_query, start=start, end=end, time_step=time_step, show_runtimes=show_runtimes
                        )

                    # graph is defined by 2 queries --> query both, add both partial graphs to get the final graph
                    else:
                        # each partial query is for some metric read+write values
                        updated_read_query = self._update_query_for_requery(query_pair[0], pod)
                        updated_write_query = self._update_query_for_requery(query_pair[1], pod)

                        # get the metric's data by adding the read values df to the write values df
                        read_graph = self._generate_graph_df(
                            graph_title, updated_read_query, start=start, end=end, time_step=time_step, show_runtimes=show_runtimes
                        )
                        write_graph = self._generate_graph_df(
                            graph_title, updated_write_query, start=start, end=end, time_step=time_step, show_runtimes=show_runtimes
                        )
                        
                        # keep time the same, so only add the values column
                        graph_df = read_graph
                        graph_df[graph_title] = read_graph[graph_title] + write_graph[graph_title]

                    # add graph to requeried_graphs_dict
                    requeried_graphs_dict[graph_title][category].append(graph_df)
        
        return requeried_graphs_dict
