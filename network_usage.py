from utils import *


class NetworkUsage():
	def __init__(self):
		self.result = {
			"Pod": None,
			"Current Receive Bandwidth":None,
			"Current Transmit Bandwidth":None,
			"Rate of Received Packets":None,
			"Rate of Transmitted Packets":None,
			"Rate of Received Packets Dropped":None,	
			"Rate of Transmitted Packets":None
		}

	def get_timed_table_row(self, pod, ts):
		#assemble queries for the given pod
		queries_dict = {
			"Current Receive Bandwidth":'sum(irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))',
			"Current Transmit Bandwidth":'sum(irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))',
			"Rate of Received Packets":'sum(irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))',
			"Rate of Transmitted Packets":'sum(irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))',
			"Rate of Received Packets Dropped":'sum(irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))',	
			"Rate of Transmitted Packets":'sum(irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'
		}

		#fill in self.result
		self.result["Pod"] = pod
		for query_title, query in queries_dict.items():
			self.result[query_title] = query_value(query)

		
		return self.result