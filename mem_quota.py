from utils import *

class MemQuota():
	def __init__(self):
		self.result = {
			"Pod": None,
			"Memory Usage":None, 	
			"Memory Requests":None, 
			"Memory Requests %":None, 
			"Memory Limits":None, 
			"Memory Limits %":None, 
			"Memory Usage (RSS)":None, 
			"Memory Usage (Cache)":None, 
			"Memory Usage":None
		}

	def get_table_row(self, pod):
		#assemble queries for the given pod
		queries_dict = {
			"Memory Usage":'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", container!="", image!="", pod="' + pod + '"})', 	
			"Memory Requests":'sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire", pod="' + pod + '"})', 
			"Memory Limits":'sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire", pod="' + pod + '"})', 
			"Memory Usage (RSS)":'sum(container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", pod="' + pod + '"})', 
			"Memory Usage (Cache)":'sum(container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", pod="' + pod + '"})', 
			"Memory Usage":'sum(container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", pod="' + pod + '"})'
		}

		#fill in self.result for non percentage keys
		for query_title, query in queries_dict.items():
			self.result[query_title] = query_value(query)

		#fill in self.result with percentages and pod
		self.result["Pod"] = pod
		self.result['Memory Requests %'] = get_percent(self.result['Memory Usage'], self.result['Memory Requests'])
		self.result['Memory Limits %'] = get_percent(self.result['Memory Usage'], self.result['Memory Limits'])
		
		return self.result