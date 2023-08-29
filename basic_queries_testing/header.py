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
from termcolor import colored


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

	#returns a dataframe containing nodes, pods, and values for a given json_data from a query (header data)
	def _generate_df(self, col_title, raw_json_data):
		#parse json data and initialize dataframe
		res_list = get_result_list(raw_json_data)
		df = pd.DataFrame(columns = ['Node', 'Pod', col_title])
		#fill in dataframe
		for datapoint in res_list:
			#each triplet is a dictionary with node (str), pod (str), values (list)
			node = datapoint['metric']['node']
			pod = datapoint['metric']['pod']
			# timestamp = datapoint['value']['0']
			value = float(datapoint['value'][1])*100 #multiply by 100 to get value in % form instead of decimal form.
			#add row to the end of the dataframe containing the node, pod, and value
			df.loc[len(df.index)] = [node, pod, value]
		return df

	#returns a dict in the form {header_title:dataframe} where the dataframe contains header values per node, pod
	def get_header_dict(self):
		header_dict = {}
		#generate a dataframe for each header item, then add it to header_dict
		for query_title, query in self.queries.items():
			json_data = query_api_site(query)
			header_dict[query_title] = self._generate_df(query_title, json_data)
		return header_dict


