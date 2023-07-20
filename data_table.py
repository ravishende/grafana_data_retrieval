from utils import *
import pandas as pd
from pprint import pprint

class DataTable():

	def cpu_quota(self):
		#dictionary storing all queries besides percentages and pods
		queries_dict = {
			'CPU Usage':'sum by(pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"})',
			'CPU Requests':'sum by(pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire"})',
			'CPU Limits':'sum by(pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire"})'
		}

		#create a final dictionary for storing columns and their titles
		col_names = ["Pod", "CPU Usage", "CPU Requests", "CPU Requests %", "CPU Limits", "CPU Limits %"]
		result = {title:None for title in col_names}

		#get the table columns for each header
		response = "" #for getting pods later without another query
		for col_title, query in queries_dict.items():
			response = get_result_list(query_api_site(query))
			result[col_title] = [res['value'][1] for res in response]

		#fill in missing values (percentages and pods)
		result["Pod"] = [res['metric']['pod'] for res in response]
		result["CPU Requests %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["CPU Usage"], result["CPU Requests"])]
		result["CPU Limits %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["CPU Usage"], result["CPU Limits"])]

		return result


	def mem_quota(self):
		#dictionary storing all queries besides percentages and pods
		queries_dict = {
			"Memory Usage":'sum by(pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", container!="", image!=""})', 	
			"Memory Requests":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire"})', 
			"Memory Limits":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire"})', 
			"Memory Usage (RSS)":'sum by(pod) (container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})', 
			"Memory Usage (Cache)":'sum by(pod) (container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})', 
			"Memory Usage":'sum by(pod) (container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})'
		}
		
		#create a final dictionary for storing columns and their titles
		col_names = ["Pod", "Memory Usage","Memory Requests",  "Memory Requests %",  "Memory Limits", "Memory Limits %", "Memory Usage (RSS)", "Memory Usage (Cache)", "Memory Usage"]
		result = {title:None for title in col_names}

		
		#get the table columns for each header and
		response = None #for getting pods later without another query
		for col_title, query in queries_dict.items():
			response = get_result_list(query_api_site(query))
			result[col_title] = [res['value'][1] for res in response]

		result["Pod"] = [res['metric']['pod'] for res in response]
		result["Memory Requests %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["Memory Usage"], result["Memory Requests"])]
		result["Memory Limits %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["Memory Usage"], result["Memory Limits"])]
		
		df = pd.DataFrame(result, columns=col_names)
		return df


	def network_quota(self, duration):
		col_names = ["Pod","Current Receive Bandwidth", "Current Transmit Bandwidth", "Rate of Received Packets", "Rate of Transmitted Packets", "Rate of Received Packets Dropped", "Rate of Transmitted Packets"]

		#assemble queries for the given pod
		queries_dict = {
			"Current Receive Bandwidth":'sum by(pod) (irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire"}[' + ts + ']))',
			"Current Transmit Bandwidth":'sum by(pod) (irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire"}[' + ts + ']))',
			"Rate of Received Packets":'sum by(pod) (irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire}[' + ts + ']))',
			"Rate of Transmitted Packets":'sum by(pod) (irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire}[' + ts + ']))',
			"Rate of Received Packets Dropped":'sum by(pod) (irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire"}[' + ts + ']))',	
			"Rate of Transmitted Packets":'sum by(pod) (irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire"}[' + ts + ']))'
		}

		#fill in self.result
		self.result["Pod"] = pod
		for query_title, query in queries_dict.items():
			self.result[query_title] = query_value(query)

		
		return self.result




