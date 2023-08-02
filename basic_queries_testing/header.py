import sys
sys.path.append("..")
from utils import *
from termcolor import cprint, colored
from rich import print as printc


header_queries = {
	'CPU Utilisation (from requests)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
	# 'CPU Utilisation (from requests)':'label_replace(sum by(pod, node) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}),"node", "$1", "pod", "(.*)") / sum by(pod, node) (kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
	# 'CPU Utilisation (from requests)': 'rate(container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
	# 'CPU Utilisation (from requests)': 'sum(container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION+ '])'
}

def find_headers_json(query_dict=header_queries):
	json_responses = {}

	for query_title, query in query_dict.items():
		#get data
		json_data = query_api_site(query)
		res_list = get_result_list(json_data)
		#handle if there isn't data
		if len(res_list) == 0:
			json_responses[query_title] = None
		else:
			#add data to dictionary
			json_responses[query_title] = json_data

	return json_responses


def print_header_json():
	json_responses = find_headers_json()
	for header_title, json_data in json_responses.items():
		print("\n\n", colored(header_title, "blue"))
		if json_data != None:
			printc(json_data)
		else:
			print(colored("No data", "red"))
