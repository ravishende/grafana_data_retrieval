import sys
import os
#get set up to be able to import files from parent directory (grafana_data_retrieval)
#for example, utils.py not in this current directory and instead in the parent
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("header.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
#imports used in file
from utils import *
from termcolor import cprint, colored
from rich import print as printc


class Header():
	def __init__(self):
		self.header_queries = {
			'CPU Utilisation (from requests)': 'container_cpu_usage_seconds_total{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION + ']',
			# sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})
			# 'CPU Utilisation (from requests)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
			# 'CPU Utilisation (from requests)':'label_replace(sum by(pod, node) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}),"node", "$1", "pod", "(.*)") / sum by(pod, node) (kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
			# 'CPU Utilisation (from requests)': 'rate(container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
			# 'CPU Utilisation (from requests)': 'sum(container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}[' + DEFAULT_DURATION+ '])'
		}


	def print_header_json(self, sorted_by_pod=True):
		headers_json_dict = self._find_headers_json()
		#loop through all the headers and their returned json data
		for header_title, raw_json_data in headers_json_dict.items():
			print("\n\n______________________________________________________________________________\n")
			print("\t\t", colored(header_title, "magenta"))
			print("______________________________________________________________________________")
			
			#handle if there is no data
			if raw_json_data == None:
				print(colored("No data", "red"))
				continue

			#parse the raw json data for the attributes to keep
			parsed_json_data = self._parse_json_data(raw_json_data)
			
			#print out the data (sorted by pod or not)
			if not sorted_by_pod:
				printc(parsed_json_data)
			else:
				#sort the response by node
				sorted_json_response = self._sort_by_node(parsed_json_data)
				#print out the sorted response
				for node, node_data in sorted_json_response.items():
					print("\n\nNode:", colored(node, "blue"))
					printc(node_data)
				
				

	#returns a dict in the form of {query_title:json_data}
	def _find_headers_json(self):
		json_responses = {}
		for query_title, query in self.header_queries.items():
			#get data and put it in the new dictionary to be returned
			json_data = query_api_site(query)
			json_responses[query_title] = json_data
		return json_responses


	#returns a filtered version of json data as a list of dictionaries containing pod, node, and values
	def _parse_json_data(self, json_data):
		res_list = get_result_list(json_data)
		parsed_data = []
		for data_dict in res_list:
			# printc(data_dict)
			pod = data_dict['metric']['pod']
			node = data_dict['metric']['node']
			values_list = data_dict['values']
			assembled_data = {'node':node, 'pod':pod, 'values':values_list}
			parsed_data.append(assembled_data)
		return parsed_data


	#sorts a filtered json dataset to group the sets by node
	def _sort_by_node(self, parsed_json_data):
		sorted_dict = {}
		for data_dict in parsed_json_data:
			data_node = data_dict['node']
			pod = data_dict['pod']
			values = data_dict['values']
			if data_node in sorted_dict.keys():
				sorted_dict[data_node]['pods'].append(pod)
				sorted_dict[data_node]['values_lists'].append(values)
			else:
				sorted_dict[data_node] = {'pods':[pod], 'values_lists':[values]}
		return sorted_dict

		



