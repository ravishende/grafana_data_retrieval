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

	def _get_values_list(self, query):
		result_list = get_result_list(query_api_site_for_graph(query))[0]['values']
		# # TODO: connect this with a passed in 'end' variable into get_result_list(query_api_site_for_graph(query, assemble_time_filter(end=end)))[0]['values'] and subtract time_ago from that end time
		# # add time context into result list
		# for i in range(len(result_list)):
		# 	time_ago = str(int(DEFAULT_GRAPH_STEP[:-1])*i) + DEFAULT_GRAPH_STEP[-1] + " ago"
		# 	result_list[i].append(time_ago)
		return result_list

		

	def print_graphs(self):
		print("\n\n_____________________________\nGraphs Without Pods\n_____________________________")
		for query_title, query in self.queries_dict_no_pods.items():
			print("\n\n", query_title)
			printc(self._get_values_list(query))
			# printc(query_api_site_for_graph(query))

		# # TODO: debug and figure out what about this query is breaking the code despite not having any pods.
		# IOPSquery = 'ceil(sum(rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + '])))'
		# print("\n\n\n*******************************")
		# pprint(query_api_site_for_graph(IOPSquery))
		# print("*******************************\n\n\n")
		

		# #TODO: add functionality for queries with pods
		# print("\n\n_____________________________\nGraphs With Pods\n_____________________________")
		# for query_title, query in self.queries_dict_with_pods.items():
		# 	print("\n\n", query_title)
		# 	printc(get_result_list(query_api_site_for_graph(query)))








