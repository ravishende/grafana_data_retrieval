from utils import *
from inputs import *
import pandas as pd
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta


class Graph():

	def __init__(self, namespace=NAMESPACE, duration=DEFAULT_DURATION):
		self.queries_dict = {
			'CPU Usage':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"})',
			'Memory Usage (w/o cache)':'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + NAMESPACE + '", container!="", image!=""})',
			'Receive Bandwidth':'sum(irate(container_network_receive_bytes_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Transmit Bandwidth':'sum(irate(container_network_transmit_bytes_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION +']))',
			'Rate of Received Packets':'sum(irate(container_network_receive_packets_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Rate of Transmitted Packets':'sum(irate(container_network_transmit_packets_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Rate of Received Packets Dropped':'sum(irate(container_network_receive_packets_dropped_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Rate of Transmitted Packets Dropped':'sum(irate(container_network_transmit_packets_dropped_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))'
		# working queries 
		  # - just need to add them to get the IOPS(Reads+Writes) that we're looking for. But it would take 2 queries instead of the 1 that we're hoping for
		  # - same thing with the Throughput(Read+Write).NOTE: WHEN ADDING MORE QUERIES, REMEMBER TO ADD A COMMA TO THE LAST QUERY FROM BEFORE
			# 'IOPS(Write)':'ceil(sum by(pod) (rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + '])))',
			# 'IOPS(Read)':'ceil(sum by(pod) (rate(container_fs_reads_total{container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + ))'
			#'ThroughPut(Read)':'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			#'ThroughPut(Write)':'sum by(pod) (rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
		#not working queries - it doesn't like the '+' between the reads and writes 
			# 'IOPS(Read+Write)':'ceil(sum by(pod) (rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + '])))'
			# 'ThroughPut(Read+Write)':'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + duration + ']))',
		}



	
	#given a datetime object (end) and a string (time_offset) (e.g. "15m"), return a new date_time object time_offset away from the end time
	def _find_time_from_offset(self, end, time_offset):
		#split time_offset to get the value and unit
		offset_value = int(time_offset[:-1])
		offset_unit = time_offset[-1]

		#return a datetime object for the start time (offset away from end time)
		if offset_unit == 'w':
			return end - timedelta(weeks = offset_value)

		elif offset_unit == 'd':
			return end - timedelta(days = offset_value)

		elif offset_unit == 'h':
			return end - timedelta(hours = offset_value)

		elif offset_unit == 'm':
			return end - timedelta(minutes = offset_value)

		elif offset_unit == 's':
			return end - timedelta(seconds = offset_value)
		#raise error if time unit is not what we are expecting
		else:	
			raise TypeError (f'\n\nBad time_offset string: \n{time_offset}\n\n')



	#assembles string for the time filter to be passed into query_api_site_for_graph()
	#takes in datetime objects for end, int for time_period_hours, and string in the form of "5h" or "2d" for step.
	def _assemble_time_filter(self, end, time_offset, step):
		#calculate start time
		start = self._find_time_from_offset(end, time_offset)
		#assemble strings
		end_str = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		start_str = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		#combine strings into time filter format
		time_filter = f'start={start_str}&end={end_str}&step={step}'

		return time_filter



	#get a list of all the values and add a column for timestamps
	def _get_values_list(self, query, end=datetime.now(), time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP, show_time_as_timestamp=True):
		time_filter = self._assemble_time_filter(end, time_offset, time_step)
		result_list = get_result_list(query_api_site_for_graph(query, time_filter))[0]['values']

		# add time context into result list
		for i in range(len(result_list)):
			#get rid of unnecessary extra info
			result_list[i].pop(0)
			result_list[i][0] = clean_round(result_list[i][0])

			#calculate time offset
			time_offset_value = i*int(time_step[:-1])
			time_offset = str(time_offset_value) + time_step[-1]
			#find new time_stamp and add it to result
			time_stamp = self._find_time_from_offset(end, time_offset)
			
			if(show_time_as_timestamp):
				#adds the time in a format for printing in a more readable way
				result_list[i].append(time_stamp.strftime("%Y-%m-%d %H:%M:%S.%f"))
			else:
				#adds the time as a datetime object for accessing/manipulating the time more easily
				result_list[i].append(time_stamp)
		

		return result_list

	def get_graphs(self, show_time_as_timestamp=True):
		graphs = {}
		for query_title, query in self.queries_dict.items():
			graphs[query_title] = self._get_values_list(query, show_time_as_timestamp=show_time_as_timestamp)
		return graphs


	#print the values list from _get_values_list
	def print_graphs(self, show_time_as_timestamp=True):
		for query_title, query in self.queries_dict.items():
			print("\n\n____________________________________________________") 
			print("\t", colored(query_title, 'magenta'))
			print("____________________________________________________") 
			printc(self._get_values_list(query, show_time_as_timestamp=show_time_as_timestamp))