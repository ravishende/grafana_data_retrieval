from utils import *

column_names=["CPU Usage","CPU Requests",'CPU Requests %', 'CPU Limits']

class MemQuota():
	def __init__(self):
		pass

	def _get_cpu_usage(self,pod):
		cpu_usage = query_api_site('sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire", pod="'+pod+'"})')
		cpu_usage = cpu_usage.json()['data']['result']
		
		# if error retrieving from api, ping again recursively
		if (len(cpu_usage) == 0):

			return get_cpu_usage(pod)

		return float(cpu_usage[0]['value'][1])

	def mem_quota(self, pod):
		result = {
			"CPU Usage":None, 
			"CPU Requests":None, 
			'CPU Requests %':None, 
			'CPU Limits':None
		}
		result["CPU Usage"] = self._get_cpu_requests()
		return result

	def _get_cpu_requests(self,pod):
		pass

	def _get_cpu_requests_percent(self,pod):
		pass

	def _get_cpu_limits(self,pod):
		pass


