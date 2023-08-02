from utils import *
from inputs import *
import pandas as pd
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta


class Graph():

	def __init__(self, namespace=NAMESPACE, end=datetime.now(), duration=DEFAULT_DURATION, time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP):
		#variables for querying data for graphs
		self.namespace = namespace
		self.end = end
		self.duration = duration
		self.time_offset = time_offset
		self.time_step = time_step

		#dict storing titles and their queries
		self.queries_dict = {
			# 'CPU Usage':'label_replace(sum by(pod, node) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}), "node", "$1", "pod", "(.*)")',
			# 'CPU Usage':'sum by(pod, node) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"})'
			'CPU Usage':'sum by(pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"})',
			'Memory Usage (w/o cache)':'sum by(pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '", container!="", image!=""})',
			'Receive Bandwidth':'sum by(pod) (irate(container_network_receive_bytes_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Transmit Bandwidth':'sum by(pod) (irate(container_network_transmit_bytes_total{namespace="' + self.namespace + '"}[' + self.duration +']))',
			'Rate of Received Packets':'sum by(pod) (irate(container_network_receive_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Transmitted Packets':'sum by(pod) (irate(container_network_transmit_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Received Packets Dropped':'sum by(pod) (irate(container_network_receive_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Transmitted Packets Dropped':'sum by(pod) (irate(container_network_transmit_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
		}
		self.partial_queries_dict = {
			# - just need to add them to get the IOPS(Reads+Writes) that we're looking for. But it would take 2 queries instead of the 1 that we're hoping for
		  	# - same thing with the Throughput(Read+Write).NOTE: WHEN ADDING MORE QUERIES, REMEMBER TO ADD A COMMA TO THE LAST QUERY FROM BEFORE
			'IOPS(Read+Write)':[
				'ceil(sum by(pod) (rate(container_fs_reads_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']) + ))', 
				'ceil(sum by(pod) (rate(container_fs_writes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + '])))'
			],
			'ThroughPut(Read+Write)':[
				'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
				'sum by(pod) (rate(container_fs_writes_bytes_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
			]
		}

		#dataframe storing all the data for the graphs
		# self.graphs_df = pd.DataFrame(data={}, columns=self.queries_dict.keys().join(self.partial_queries_dict.keys()))
		self.graphs_df = pd.DataFrame()



	#given a datetime object (end) and a string (time_offset) (e.g. "12h5m30s"), return a new date_time object time_offset away from the end time
	def _find_time_from_offset(self, end_time, offset):
		#get the offset in a usable form
		time_dict = get_time_dict_from_str(offset)
		#create new datetime timedelta to represent the time offset and pass in parameters as values from time_dict
		t_offset = timedelta(**time_dict)
		#return the start time
		return end_time-t_offset



	#assembles string for the time filter to be passed into query_api_site_for_graph()
	def _assemble_time_filter(self):
		#calculate start time
		start = self._find_time_from_offset(self.end, self.time_offset)
		#assemble strings
		end_str = self.end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		#combine strings into time filter format
		time_filter = f'start={start_str}&end={end_str}&step={self.time_step}'

		return time_filter



	#get a list of all the values and add a column for timestamps
	def _generate_graph_data(self, query):
		#create time filter to then generate list of all datapoints for the graph
		time_filter = self._assemble_time_filter()
		result_list = get_result_list(query_api_site_for_graph(query, time_filter))
		
		df_graph_times_list = []
		df_graph_values_list = []
		df_graph_pods_list = []


		#loop through pods
		for i in range(len(result_list)):
			#prepare data to be extracted
			times_values_list = result_list[i]['values']	
			
			df_values_list = []
			df_times_list = []
			
			#fill in lists
			for time_value_pair in times_values_list:
				df_times_list.append(time_value_pair[0])
				df_values_list.append(float(time_value_pair[1]))
			df_pod_list = [result_list[i]['metric']['pod']]*len(df_times_list)
			
			#add pod's lists to the whole graph's lists
			df_graph_times_list.extend(df_times_list)
			df_graph_values_list.extend(df_values_list)
			df_graph_pods_list.extend(df_pod_list)

			
			# #go through the values list of each pod and clean up time and value
			# for j in range(len(times_values_list)):
			# 	#round values
			# 	times_values_list[j][1] = clean_round(float(times_values_list[j][1]))

			# 	#display the time in each datapoint how the user specified.
			# 	#the time is queried in seconds since the epoch (01/01/1970) so if user wants to display as seconds, do nothing.
			# 	if display_time_as != "seconds":
			# 		#get seconds to a datetime that can be used 
			# 		date_time = datetime.fromtimestamp(times_values_list[j][0])
					
			# 		if display_time_as == "datetime":
			# 			#Display timestamps as datetimes
			# 			times_values_list[j][0] = date_time

			# 		elif display_time_as == "timestamp":
			# 			#formats the time as a string for printing in a more readable way
			# 			times_values_list[j][0] = (date_time.strftime("%Y-%m-%d %H:%M:%S.%f"))

			# 		else:
			# 			#input was not one of the options. 
			# 			raise ValueError('display_time_as can only be set to one of the following: "datetime", "timestamp", or "seconds".')

			# # make sure the result list that is returned has the new updates
			# result_list[i]['values'] = times_values_list

		return df_graph_times_list, df_graph_values_list, df_graph_pods_list



	# #get a dictionary in the form of {graph titles: list of graph data}
	# def get_graphs(self, display_time_as=DEFAULT_GRAPH_DISPLAY_TIME):
	# 	#get all of the initial graphs from the normal queries		
	# 	graphs_dict = {}
	# 	for query_title, query in self.queries_dict.items():
	# 		graphs_dict[query_title] = self._generate_formated_result_list(query, display_time_as=display_time_as)
		
	# 	#combine the partial queries in partial_queries_dict so that each list of two queries is one datapoint in graphs_dict
	# 	for query_title, query_pair in self.partial_queries_dict.items():
			
	# 		#store the two queries' values
	# 		read_data = self._generate_formated_result_list(query_pair[0], display_time_as=display_time_as)
	# 		write_data = self._generate_formated_result_list(query_pair[1], display_time_as=display_time_as)
			
	# 		#loop through each dataset of a pod
	# 		for pod_i in range(len(read_data)):
	# 			#loop through the data points in a pod's graph
	# 			for datapoint_i in range(len(read_data[pod_i]['values'])):
	# 				#add the write_data value to the read_data value for each datapoint in graphs_dict[]
	# 				read_data[pod_i]['values'][datapoint_i][1] += write_data[pod_i]['values'][datapoint_i][1]

	# 		#put the newly modified read_data (which is now read+write data) into graphs_dict.
	# 		graphs_dict[query_title] = read_data

	# 	return graphs_dict

	#get a dictionary in the form of {graph titles: list of graph data}
	def _generate_graphs(self):
		graphs = []
		#get all of the initial graphs from the normal queries
		for query_title, query in self.queries_dict.items():
			#collect graph data
			times, values, pods = self._generate_graph_data(query)
			#make and populate dataframe, then add to graphs
			graph_df = pd.DataFrame()
			graph_df['Time'] = times
			graph_df['Pod'] = pods
			graph_df[query_title] = values
			graphs.append(graph_df)
		#get graphs from partial queries
		for query_title, query_pair in self.partial_queries_dict.items():
			#store the two queries' values
			times, read_values, pods = self._generate_graph_data(query_pair[0])
			write_values = self._generate_graph_data(query_pair[1])[1]
			
			graph_df = pd.DataFrame()
			graph_df['Time'] = times
			graph_df['Pod'] = pods
			graph_df[query_title] = [read_vals + write_vals for read_vals, write_vals in zip(read_values, write_values)]
			
			#put the newly modified read_data (which is now read+write data) into graphs_dict.
			graphs.append(graph_df)
		return graphs



	def get_graphs(self, display_time_as_timestamp=True):
		graphs = self._generate_graphs()
		for graph in graphs:
			#update graphs with correct time columns
			if display_time_as_timestamp:
				graph['Time'] = pd.to_datetime(graph['Time'], unit="s")
			#after time is in correct format, set it to be the index.
			graph.set_index('Time', inplace=True)
		return graphs

