import requests
import json

ts = '[15m]'

QUERIES = {
	'CPU Utilisation (from requests)':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',
	'CPU Utilisation (from limits)':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',
	'Memory Utilisation (from requests)':'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})',
	'Memory Utilisation (from limits)':'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})'
}

QUERIES_WITH_TIME = {
	'A':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"})' + ts +' / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"}' + ts + ')',
	'B':'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"})' + ts + ' / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"}' + ts + ')',
	'C':'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}' + ts + ') / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"}' + ts + ')',
	'D':'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}' + ts + ') / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"}' + ts + ')'
}

def find_query_values():
	query_values = {}

	for query_title, query in QUERIES.items():
		try:
			query_values[query_title] = query_api_site(query).json()['data']['result'][0]['value']
		except KeyError:
			query_values[query_title] = "no data/failed response"
		except IndexError:
			query_values[query_title] = "no data/failed response"

	return query_values


def print_query_values():
	query_values = find_query_values()
	for query_title, value in query_values.items():
		print(f'{query_title}: \n\t\t{value}\n')


def query_api_site(query=QUERIES['CPU Utilisation (from requests)']):
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	query = 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})'
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