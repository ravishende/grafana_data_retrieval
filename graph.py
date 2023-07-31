from utils import *
from inputs import *
import pandas as pd
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta


class Graph():

	def __init__(self, namespace=NAMESPACE, end=datetime.now(), duration=DEFAULT_DURATION, time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP):
		#define variables for querying data for graphs
		self.namespace = namespace
		self.end = end
		self.duration = duration
		self.time_offset = time_offset
		self.time_step = time_step


		#dict storing titles and their queries
		self.queries_dict = {
			'CPU Usage':'sum by(pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"})',
			'Memory Usage (w/o cache)':'sum by(pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '", container!="", image!=""})',
			'Receive Bandwidth':'sum by(pod) (irate(container_network_receive_bytes_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Transmit Bandwidth':'sum by(pod) (irate(container_network_transmit_bytes_total{namespace="' + self.namespace + '"}[' + self.duration +']))',
			'Rate of Received Packets':'sum by(pod) (irate(container_network_receive_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Transmitted Packets':'sum by(pod) (irate(container_network_transmit_packets_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Received Packets Dropped':'sum by(pod) (irate(container_network_receive_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Transmitted Packets Dropped':'sum by(pod) (irate(container_network_transmit_packets_dropped_total{namespace="' + self.namespace + '"}[' + self.duration + ']))'
		# working queries 
		  # - just need to add them to get the IOPS(Reads+Writes) that we're looking for. But it would take 2 queries instead of the 1 that we're hoping for
		  # - same thing with the Throughput(Read+Write).NOTE: WHEN ADDING MORE QUERIES, REMEMBER TO ADD A COMMA TO THE LAST QUERY FROM BEFORE
			# 'IOPS(Write)':'ceil(sum by(pod) (rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + '])))',
			# 'IOPS(Read)':'ceil(sum by(pod) (rate(container_fs_reads_total{container!="", namespace="' + self.namespace + '"}[' + self.duration + ']) + ))'
			#'ThroughPut(Read)':'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			#'ThroughPut(Write)':'sum by(pod) (rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + ']))',
		#not working queries - it doesn't like the '+' between the reads and writes 
			# 'IOPS(Read+Write)':'ceil(sum by(pod) (rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + ']) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + '])))'
			# 'ThroughPut(Read+Write)':'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + ']) + rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + self.namespace + '"}[' + self.duration + ']))',
		}




	#given a datetime object (end) and a string (time_offset) (e.g. "12h5m30s"), return a new date_time object time_offset away from the end time
	def _find_time_from_offset(self, end_time, offset):
		#get the offset in a usable form
		time_dict = get_time_dict_from_str(offset)
		#create new datetime timedelta to represent the time offset and pass in parameters as values from time_dict
		t_offset = timedelta(**time_dict)
		#return the start time
		return end_time-t_offset



	#assembles string for the time filter to be passed into query_api_site_for_graph()
	#takes in datetime objects for end, int for time_period_hours, and string in the form of "5h" or "2d" for step.
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
	def _generate_formated_result_list(self, query, display_time_as):
		#create time filter to then generate list of all datapoints for the graph
		time_filter = self._assemble_time_filter()
		result_list = get_result_list(query_api_site_for_graph(query, time_filter))
		
		#loop through the data for each pod
		for i in range(len(result_list)):
			values_list = result_list[i]['values']
		
			#go through the values list of each pod and clean up time and value
			for j in range(len(result_list[i]['values'])):
				#round values
				values_list[j][1] = clean_round(float(values_list[j][1]))

				#display the time in each datapoint how the user specified.
				#the time is queried in seconds since the epoch (01/01/1970) so if user wants to display as seconds, do nothing.
				if display_time_as != "seconds":
					#get seconds to a datetime that can be used 
					date_time = datetime.fromtimestamp(values_list[j][0])
					
					if display_time_as == "datetime":
						#Display timestamps as datetimes
						values_list[j][0] = date_time

					elif display_time_as == "timestamp":
						#formats the time as a string for printing in a more readable way
						values_list[j][0] = (date_time.strftime("%Y-%m-%d %H:%M:%S.%f"))

					else:
						#input was not one of the options. 
						raise ValueError('display_time_as can only be set to one of the following: "datetime", "timestamp", or "seconds".')


			#make sure the result list that is returned has the new updates
			result_list[i]['values'] = values_list

		return result_list



	#get a dictionary in the form of {graph titles: list of graph data}
	def get_graphs(self, display_time_as=DEFAULT_GRAPH_DISPLAY_TIME):
		graphs = {}
		for query_title, query in self.queries_dict.items():
			graphs[query_title] = self._generate_formated_result_list(query, display_time_as=display_time_as)
		return graphs

	#print the values list received from _generate_formated_result_list for each graph
	def print_graphs(self, display_time_as=DEFAULT_GRAPH_DISPLAY_TIME):
		for query_title, query in self.queries_dict.items():
			print("\n\n____________________________________________________") 
			print("\t", colored(query_title, 'magenta'))
			print("____________________________________________________") 
			printc(self._generate_formated_result_list(query, display_time_as=display_time_as))

		print("\n\n\n")



