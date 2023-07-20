from utils import *
import pandas as pd
from pprint import pprint

class DataTable():

	def cpu_quota(self):
		pass
		
		# queries_dict = {
		# 	'CPU Usage':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire", pod="'+pod+'"})',
			
		# }
		# col_names = []
		# result = {title:None for title in col_names}

		# #get the table columns for each header and
		# for col_title, query in MemQuota.queries_dict.items():
		# 	result[col_title] = [res['value'][1] for res in get_result_list(query_api_site(query))]

		# #fill in missing values (percentages and pods)
		# result["Pod"] = get_pods_list()
		# result["Memory Requests %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["Memory Usage"], result["Memory Requests"])]
		# result["Memory Limits %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["Memory Usage"], result["Memory Limits"])]

		# return result


	def mem_quota(self):
		queries_dict = {
			"Memory Usage":'sum by(pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", container!="", image!=""})', 	
			"Memory Requests":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire"})', 
			"Memory Limits":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire"})', 
			"Memory Usage (RSS)":'sum by(pod) (container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})', 
			"Memory Usage (Cache)":'sum by(pod) (container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})', 
			"Memory Usage":'sum by(pod) (container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!=""})'
		}
		
		col_names = ["Pod", "Memory Usage","Memory Requests",  "Memory Requests %",  "Memory Limits", "Memory Limits %", "Memory Usage (RSS)", "Memory Usage (Cache)", "Memory Usage"]
		result = {title:None for title in col_names}

		#get the table columns for each header and
		response = None
		for col_title, query in queries_dict.items():
			response = get_result_list(query_api_site(query))
			result[col_title] = [res['value'][1] for res in response]

		result["Pod"] = [i['metric']['pod'] for i in response]

		result["Memory Requests %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["Memory Usage"], result["Memory Requests"])]
		result["Memory Limits %"] = [get_percent(float(usage), float(requests)) for usage,requests in zip(result["Memory Usage"], result["Memory Limits"])]
		
		df = pd.DataFrame(result, columns=col_names)
		return df


	def network_quota(self, duration):
		pass


d = DataTable()
# d.mem_quota()
print(d.mem_quota().to_string())
# d.cpu_quota()
# d.network_quota(duration="4h")
