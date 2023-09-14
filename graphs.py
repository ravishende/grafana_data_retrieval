#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
from utils import query_api_site_for_graph, get_result_list, filter_df_for_workers, print_heading, print_title, print_sub_title
from inputs import NAMESPACE, DEFAULT_DURATION, DEFAULT_GRAPH_TIME_OFFSET, DEFAULT_GRAPH_STEP, REQUERY_GRAPH_STEP_DIVISOR
from datetime import datetime, timedelta
from termcolor import colored
from pprint import pprint
from tqdm import tqdm
import time
import re


class Graphs():
    def __init__(self, namespace=NAMESPACE, end=datetime.now(), duration=DEFAULT_DURATION, time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP, requery_step_divisor=REQUERY_GRAPH_STEP_DIVISOR):
        # variables for querying data for graphs
        self.namespace = namespace
        self.end = end
        self.duration = duration
        self.time_offset = time_offset
        self.time_step = time_step
        self.requery_step_divisor = requery_step_divisor

        # dict storing titles and their queries.
        self.queries_dict = { # Note: Do not change the white space in 'sum by(node, pod) ' because _update_query_for_requery() relies on it
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

    #given a timedelta, get it in the form 2d4h12m30s for use with querying (time_step)
    def _get_time_str_from_timedelta(self, delta):
        days = delta.days
        hours, remainder = divmod(delta.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = f"{days}d{hours}h{minutes}m{seconds}s"
        return time_str

    # given a string in the form 5w3d6h30m5s, save the times to a dict accesible
    # by the unit as their key. The int times can be any length (500m160s is allowed)
    # works given as many or few of the time units. (e.g. 12h also works and sets everything but h to None)
    def _get_timedelta_from_str(self, time_str):
        # define regex pattern (groups by optional int+unit but only keeps the int)
        pattern = "(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
        feedback = re.search(pattern, time_str)

        # save time variables (if not in time_str they will be set to None)
        w, d, h, m, s = feedback.groups()
        # put time variables into a dictionary
        time_dict = {
            'weeks': w,
            'days': d,
            'hours': h,
            'minutes': m,
            'seconds': s
        }

        # get rid of null values in time_dict
        time_dict = {
            unit: float(value) for unit, value
            in time_dict.items() if value is not None
        }
        # create new datetime timedelta to represent the time
        # and pass in parameters as values from time_dict
        time_delta = timedelta(**time_dict)

        return time_delta

    # given an end_time (datetime) and an offset_str (string) (e.g. "12h5m30s"),
    # return a new datetime object offset away from the end_time
    def _find_time_from_offset(self, end_time, offset_str):
        # get the offset in a usable form: {..., 'hours':____, 'minutes':___, 'seconds':____}
        time_offset = self._get_timedelta_from_str(offset_str)
        # return the start time
        return end_time-time_offset

    # takes in a time in seconds since epoch (float or int), pandas Timestamp(), or datetime object formats
    # returns the time as a datetime object
    def _convert_to_datetime(self, time):
        # check if time is a pandas Timestamp()
        # technically this counts as an instance of type datetime but is not the same
        # so we must check if time is a pandas Timestamp() before checking if it's a datetime object
        if isinstance(time, pd.Timestamp):
            return time.to_pydatetime(warn=False)
        #check if time is a float (seconds since the epoch: 01/01/1970)
        if isinstance(time, float) or isinstance(time, int):
            return datetime.fromtimestamp(time)
        #check if time is a datetime object
        if isinstance(time, datetime):
            return time
        # if time is of unsupported type, raise error
        raise TypeError("argument for _convert_to_datetime() must be of type float, int, pandas.Timestamp, or datetime.datetime")

    # assembles string for the time filter to be passed into query_api_site_for_graph()
    def _assemble_time_filter(self, start=None, end=None, time_step=None):
        # set default values if not given
        if time_step is None:
            time_step = self.time_step
        if start is None and end is None:
            # calculate start time
            start = self._find_time_from_offset(self.end, self.time_offset)
            end = self.end
        elif start is None or end is None:  # one is none but not both
            raise ValueError("start and end must either be both defined or both None")
        else:
            # make sure both start and end are datetime objects
            start = self._convert_to_datetime(start)
            end = self._convert_to_datetime(end)
        
        # assemble strings
        end_str = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        # combine strings into time filter format
        time_filter = f'start={start_str}&end={end_str}&step={time_step}'

        return time_filter

    # returns a dataframe containing Time, Node, Pod, and value (value is titled something different for each graph)
    # returns none if there is no data
    def _generate_graph_df(self, query_title, query, start=None, end=None, time_step=None, show_runtimes=False):
        # create time filter to then generate list of all datapoints for the graph
        time_filter = self._assemble_time_filter(start=start, end=end, time_step=time_step)

        if show_runtimes:
            start = time.time()

        result_list = get_result_list(query_api_site_for_graph(query, time_filter))
        if len(result_list) == 0:
            return None

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
        queries_dict = self.queries_dict
        partial_queries_dict = self.partial_queries_dict
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

    # generate and return a list of all the graphs
    def get_graphs_dict(self, only_include_worker_pods=False, display_time_as_timestamp=True, show_runtimes=False):
        graphs_dict = self._generate_graphs(show_runtimes=show_runtimes)
        #loop through graphs
        for graph_title, graph in graphs_dict.items():
            if graph is None:
                continue

            # for every worker pod in graph, change pod's value to just be the worker id, drop all non-worker pods
            if only_include_worker_pods:
                graph = filter_df_for_workers(graph)

            # update graphs with correct time columns
            if display_time_as_timestamp:
                graph['Time'] = pd.to_datetime(graph['Time'], unit="s")

            graphs_dict[graph_title] = graph

        return graphs_dict

    def get_graphs_as_one_df(self, graphs_dict=None, only_include_worker_pods=False, display_time_as_timestamp=True, show_runtimes=False):
        total_df = pd.DataFrame(data={})

        # Generate graphs if none given
        if graphs_dict is None:
            graphs_dict = self._generate_graphs(show_runtimes=show_runtimes)

        # Fill in Node and Pod columns with the first non-empty graph
        for graph_df in graphs_dict.values():
            if graph_df is not None:
                total_df['Time'] = graph_df['Time']
                total_df['Node'] = graph_df['Node']
                total_df['Pod'] = graph_df['Pod']
                break

        # Fill in graphs columns
        for title, graph_df in graphs_dict.items():
            if graph_df is None:
                total_df[title] = None
                continue
            total_df[title] = graph_df[title]

        return total_df


# _________________________________________
#
#           Requery Methods
#__________________________________________

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
        # # change 'sum by(node, pod) ' to just be 'sum by(node) ' so we retain the node information while only requesting one pod 
        # str_to_delete = ' by(node, pod) '
        # query = query.replace(str_to_delete, ' by(node) ')

        # add specific pod to query so only the one specific pod is queried instead of all pods
        namespace_index = query.find('namespace="')
        pod_str = f'pod="{pod}", '
        updated_query = query[:namespace_index] + pod_str + query[namespace_index:]

        return updated_query

    # returns a dict in the form: 
    # {'dropped': [{'pod':str, 'start':datetime, 'end':datetime, 'prev val':float}, {...}, ...], 
    #  'recovered': [{'pod':str, 'start':datetime, 'end':datetime, 'val':float}, {...}, ...]}
    # returns none if no losses
    def _check_graph_loss(self, graph_title, graph_df, print_info=False):
        # variables
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

            # pod dropped - was nonzero, now is zero
            if previous_value != 0 and current_value == 0:
                drop = {
                    'pod': current_pod,
                    'start': graph_df['Time'][index-1],
                    'end': graph_df['Time'][index],
                    'prev val': previous_value
                }
                pods_dropped.append(drop)

            # pod recovered - was zero, now is nonzero
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

            #update old pod and value for next iteration
            previous_value = current_value
            previous_pod = current_pod

        # cannot have recoverd pods if none were dropped.
        # if both are dropped and recovered are empty, return None
        if len(pods_dropped) == 0:
            return None

        # print collected statistics
        if print_info and len(pods_dropped) > 0:
            print_sub_title(graph_title)
            # Print Info for Pods Dropped: pod, previous value, time of last value, time dropped,
            print(colored("Pods Dropped || Previous Value || Time of Previous Value || Time Dropped", "green"))
            for pod in pods_dropped:
                print(f'{pod["pod"]} || {pod["prev val"]} || {pod["start"]} || {pod["end"]}')

            if len(pods_recovered) > 0:
                # Print Info for Pods Recovered: pod, value, time of last 0, time recovered
                print(colored("\nPods Recovered || Recovered Value || Time of Previous 0 || Time Recovered", "green"))
                for pod in pods_recovered:
                    print(f'{pod["pod"]} || {pod["val"]} || {pod["start"]} || {pod["end"]}')

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

        # check for if graphs_dict was input as single dataframe instead
        if isinstance(graphs_dict, pd.DataFrame):
            graphs_dict = self.convert_graphs_df_to_dict(graphs_dict)

        # check if graphs_dict has data
        elif all(value is None for value in graphs_dict.values()):
                raise ValueError("graphs_dict has no data; can't check for losses of no data")
                return
        graphs_losses_dict = {}

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
                if graph_title in self.queries_dict.keys():
                    query = self.queries_dict[graph_title]
                    partial_query = False
                else:
                    # partial query --> 2 queries per graph
                    query_pair = self.partial_queries_dict[graph_title]
                    partial_query = True
                
                # loop through the pods for each graph
                for pod_dict in pods_list:
                    # assemble arguments for self._generate_graph_df
                    pod = pod_dict['pod']
                    start = pod_dict['start']
                    end = pod_dict['end']
                    #convert time_step to timedelta to be able to divide it
                    time_step = self._get_timedelta_from_str(self.time_step)/self.requery_step_divisor
                    #convert time_step back to str to be used for querying
                    time_step = self._get_time_str_from_timedelta(time_step)
                    graph_df = None

                    # graph is defined by 1 query --> query graph
                    if not partial_query:
                        updated_query = self._update_query_for_requery(query, pod)
                        graph_df = self._generate_graph_df(
                            graph_title, updated_query, start=start, end=end, time_step=time_step, show_runtimes=show_runtimes
                        )
                    
                    # graph is defined by 2 queries --> query both, add both partial graphs to get the final graph
                    else: 
                        updated_read_query = self._update_query_for_requery(query_pair[0], pod)
                        updated_write_query = self._update_query_for_requery(query_pair[1], pod)
                        
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