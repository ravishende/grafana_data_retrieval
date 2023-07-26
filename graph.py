from utils import *
from inputs import *
import pandas as pd
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta


class Graph():

	def __init__(self):
		self.queries_dict_no_pods = {
			'CPU Usage':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"})',
			'Memory Usage (w/o cache)':'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + NAMESPACE + '", container!="", image!=""})',
			'Receive Bandwidth':'sum(irate(container_network_receive_bytes_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Transmit Bandwidth':'sum(irate(container_network_transmit_bytes_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION +']))',
			'Rate of Received Packets':'sum(irate(container_network_receive_packets_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Rate of Transmitted Packets':'sum(irate(container_network_transmit_packets_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Rate of Received Packets Dropped':'sum(irate(container_network_receive_packets_dropped_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Rate of Transmitted Packets Dropped':'sum(irate(container_network_transmit_packets_dropped_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			# 'IOPS(Reads+Writes)':'ceil(sum(rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + '])))'

		}

		self.queries_dict_with_pods = {
			'ThroughPut(Read+Write)':'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'IOPS (Reads)':'sum by(pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'IOPS(Writes)':'sum by(pod) (rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'IOPS(Reads + Writes)':'sum by(pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Throughput(Read)':'sum by(pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Throughput(Write)':'sum by(pod) (rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))',
			'Throughput(Read + Write)':'sum by(pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']))'
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
		# TODO: raise error if time unit is not what we are expecting
		else:	
			raise TypeError (f'\n\nBad time_offset string: \n{time_offset}\n\n')



	#assembles string for the time filter to be passed into query_api_site_for_graph()
	#takes in datetime objects for end, int for time_period_hours, and string in the form of "5h" or "2d" for step.
	def _assemble_time_filter(self, end, time_offset, step):
		#calculate start time
		start = self._find_time_from_offset(end, time_offset)
		#assemble strings
		end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
		start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
		#combine strings into time filter format
		time_filter = f'start={start_str}&end={end_str}&step={step}'

		return time_filter



	#get a list of all the values and add a column for timestamps
	def get_values_list(self, query, end=datetime.now(), time_offset=DEFAULT_GRAPH_TIME_OFFSET, time_step=DEFAULT_GRAPH_STEP):
		time_filter = self._assemble_time_filter(end, time_offset, time_step)
		result_list = get_result_list(query_api_site_for_graph(query, time_filter))[0]['values']

		# add time context into result list
		for i in range(len(result_list)):
			#calculate time offset
			time_offset_value = i*int(time_step[:-1])
			time_offset = str(time_offset_value) + time_step[-1]
			#find new time_stamp and add it to result
			time_stamp = self._find_time_from_offset(end, time_offset)
			# result_list[i].append(time_stamp.strftime("%Y-%m-%d %H:%M:%S")) #for printing as a readable time
			result_list[i].append(time_stamp) #for accessing as a datetime
		
		return result_list



	#print the values list from _get_values_list
	def print_graphs(self):
		print("\n\n_____________________________\nGraphs Without Pods\n_____________________________")
		for query_title, query in self.queries_dict_no_pods.items():
			print("\n\n", query_title)
			printc(self.get_values_list(query))
			# printc(query_api_site_for_graph(query))
