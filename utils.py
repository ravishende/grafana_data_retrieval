import requests
import json

def query_api_site():
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	# query = 'rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])'
	query = 'rate(node_cpu_seconds_total[1m])'
	# query = ' sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})'
	# query = 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire"})'
	# query = 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})'
	# query = 'sum(node_namespace_pod_container:container_cpu_usage_seconds)'
	endpoint = f'query?query={query}'
	full_url = base_url + endpoint
	cpu_data = requests.get(full_url)
	return cpu_data

def write_json(data):
	with open("e.json","w") as file:
		json.dump(data.json(),file)

def read_json():
	with open("webscrape.json","r") as file:
		data = json.load(file)
	return data

"""
The following example expression returns the per-second rate of HTTP requests 
as measured over the last 5 minutes, 
per time series in the range vector:

rate(http_requests_total{job="api-server"}[5m])

rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])

rate()

container_cpu_usage_seconds_total


time = 3h
namespace "wifire-quicfire"











"""