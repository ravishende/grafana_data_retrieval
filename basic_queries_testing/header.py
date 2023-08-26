import sys
import os
#get set up to be able to import files from parent directory (grafana_data_retrieval)
#for example, utils.py not in this current directory and instead in the parent
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("header.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
#imports used in file
import pandas as pd
from utils import *
from termcolor import cprint, colored
from rich import print as printc


class Header():
	def __init__(self, namespace=NAMESPACE, duration=DEFAULT_DURATION):
		self.namespace = namespace
		self.duration = duration
		self.queries = {
			'CPU Utilisation (from requests)':'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + self.namespace + '", resource="cpu"})',
			'CPU Utilisation (from limits)': 'sum by (node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + self.namespace + '"}) / sum by(node, pod) (kube_pod_container_resource_limits{job="kube-state-metrics", namespace="' + self.namespace + '", resource="cpu"})',
			'Memory Utilisation (from requests)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '",container!="", image!=""}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + self.namespace + '", resource="memory"})',
			'Memory Utilisation (from limits)': 'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", namespace="' + self.namespace + '",container!="", image!=""}) / sum by(node, pod) (kube_pod_container_resource_limits{job="kube-state-metrics", namespace="' + self.namespace + '", resource="memory"})'
		}
				
				

	#using the self.queries dict, pings the api and returns a dict in the form of {query_title:json_data}
	def _find_headers_json(self):
		#create a final dictionary for storing columns and their titles
		json_responses = {}
		for query_title, query in self.queries.items():
			#get data and put it in the new dictionary to be returned
			json_data = query_api_site(query)
			json_responses[query_title] = json_data
		return json_responses


	#returns a filtered version of json data as a list of dictionaries containing pod, node, and values
	def _parse_json_data(self, json_data):
		res_list = get_result_list(json_data)
		parsed_data = []
		for data_dict in res_list:
			pod = data_dict['metric']['pod']
			node = data_dict['metric']['node']
			value = data_dict['value'][1] #data_dict['value'][0] is timestamp
			assembled_data = {'node':node, 'pod':pod, 'value':value}
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

		
	#prints out data of nodes, pods, and values for the queries in self.queries. These are the header values in the grafana page.
	def print_header_json(self, sorted_by_node=True):
		headers_json_dict = self._find_headers_json()
		#loop through all the headers and their returned json data
		for header_title, raw_json_data in headers_json_dict.items():
			print("\n\n\n\n______________________________________________________________________________\n")
			print("\t\t", colored(header_title, "magenta"))
			print("______________________________________________________________________________")
			
			#handle if there is no data
			if raw_json_data == None:
				print(colored("No data", "red"))
				continue

			#parse the raw json data for the attributes to keep
			parsed_json_data = self._parse_json_data(raw_json_data)
			
			#print out the data (sorted by pod or not)
			if not sorted_by_node:
				printc(parsed_json_data)
			else:
				#sort the response by node
				sorted_json_response = self._sort_by_node(parsed_json_data)
				#print out the sorted response
				for node, node_data in sorted_json_response.items():
					print("\n\nNode:", colored(node, "blue"))
					printc(node_data)

	#returns a dataframe containing nodes, pods, and values for a given json_data from a query (header data)
	def _generate_df(self, col_title, raw_json_data):
		#parse json data and initialize dataframe
		parsed_data = self._parse_json_data(raw_json_data)
		df = pd.DataFrame(columns = ['Node', 'Pod', col_title])
		#fill in dataframe
		for datapoint in parsed_data:
			#each triplet is a dictionary with node (str), pod (str), values (list)
			node = datapoint['node']
			pod = datapoint['pod']
			value = float(datapoint['value'])*100 #multiply by 100 to get it in % form instead of decimal form.
			#add a row to the end of the dataframe containing a node, pod, and value
			df.loc[len(df.index)] = [node, pod, value]
		return df

	def print_header_data(self):
		for query_title, query in self.queries.items():
			json_data = query_api_site(query)

			print("\n\n\n\n______________________________________________________________________________\n")
			print("\t\t", colored(query_title + " %", "green"))
			print("______________________________________________________________________________")
			
			#handle if there is no data
			if len(get_result_list(json_data)) == 0:
				print(colored("No data", "red"))
				continue

			print("\n", self._generate_df(query_title, json_data), "\n\n")


