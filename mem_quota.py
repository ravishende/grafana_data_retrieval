from utils import *

class MemQuota():

	queries_dict = {
		"Memory Usage":'sum by(pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", container!="", image!=""})', 	
		"Memory Requests":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire"})', 
		"Memory Limits":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire"})', 
		"Memory Usage (RSS)":'sum by(pod) (container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})', 
		"Memory Usage (Cache)":'sum by(pod) (container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})', 
		"Memory Usage":'sum by(pod) (container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})'
	}

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

	def get_table(self):
		#get the table columns for each header and
		for col_title, query in MemQuota.queries_dict.items():
			self.result[col_title] = [res[i]['value'][1] for res in get_result_list(query_api_site(query))]

		self.result["Pod"] = get_pods_list()
		self.result["Memory Requests %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(self.result["Memory Usage"], self.result["Memory Requests"])]
		self.result["Memory Limits %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(self.result["Memory Usage"], self.result["Memory Limits"])]

		return self.result

