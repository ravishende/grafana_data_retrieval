from utils import *


class CPUQuota():
	def __init__(self):
		self.result = {
			"Pod": None,
			"CPU Usage":None, 
			"CPU Requests":None, 
			"CPU Requests %":None, 
			"CPU Limits":None,
			"CPU Limits %": None
		}

	def get_table_row(self, pod):
		#assemble queries for the given pod
		queries_dict = {
			'CPU Usage':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire", pod="'+pod+'"})',
			'CPU Requests':'sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire", pod="'+pod+'"})',
			'CPU Limits':'sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire", pod="'+pod+'"})'
		}

		#fill in self.result for non percentage keys
		for query_title, query in queries_dict.items():
			self.result[query_title] = query_value(query)

		#fill in self.result with percentages and pod
		self.result["Pod"] = pod
		self.result['CPU Requests %'] = get_percent(self.result['CPU Usage'], self.result['CPU Requests'])
		self.result['CPU Limits %'] = get_percent(self.result['CPU Usage'], self.result['CPU Limits'])
		
		return self.result