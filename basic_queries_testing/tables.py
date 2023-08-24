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

class Tables():
	def __init__(self, namespace=NAMESPACE, duration=DEFAULT_DURATION):
		self.namespace = namespace
		self.duration = duration
		self.queries = {
			#CPU Quota
			'CPU Usage':'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"})',
			'CPU Requests':'sum by(node, pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="", namespace="' + NAMESPACE + '"})',
			'CPU Limits':'sum by(node, pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="", namespace="' + NAMESPACE + '"})',
			#Memory Quota
			'Memory Usage':'sum by(node, pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '", container!="", image!=""})', 	
			'Memory Requests':'sum by(node, pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="' + self.namespace + '"})', 
			'Memory Limits':'sum by(node, pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="' + self.namespace + '"})', 
			'Memory Usage (RSS)':'sum by(node, pod) (container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '",container!=""})', 
			'Memory Usage (Cache)':'sum by(node, pod) (container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '",container!=""})', 
			'Memory Usage':'sum by(node, pod) (container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '",container!=""})',
			#Network Usage
			'Current Receive Bandwidth':'sum by(node, pod) (irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Current Transmit Bandwidth':'sum by(node, pod) (irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Received Packets':'sum by(node, pod) (irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Transmitted Packets':'sum by(node, pod) (irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Received Packets Dropped':'sum by(node, pod) (irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Rate of Transmitted Packets Dropped':'sum by(node, pod) (irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
		}
		self.partial_queries = {
			'IOPS(Reads)':'sum by(node, pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'IOPS(Writes)':'sum by(node, pod) (rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Throughput(Read)':'sum by(node, pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))',
			'Throughput(Write)':'sum by(node, pod) (rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + self.namespace + '"}[' + self.duration + ']))'
		}

	#TODO: Figure out how you want to represent these value lists in a df table now that there's more than one node and pod per cell.
	#TODO: Also, add mem_quota and other tables.
	def cpu_quota(self):
		#create a final dictionary for storing columns and their titles
		col_names = ["Pod", "Node", "CPU Usage", "CPU Requests", "CPU Requests %", "CPU Limits", "CPU Limits %"]
		response_dict = {}

		#store json data from querying the api
		for col_title, query in self.queries.items():
			queried_data = query_api_site(query)
			response_dict[col_title] = get_result_list(queried_data)

		#get a list of all pods and create a row to be added to the database later
		pods = self._get_pods(response_dict)
		row = {title:None for title in col_names}
		df = pd.DataFrame({i:[] for i in col_names})

		#assemble row
		i = 0
		for pod in pods:
			#get pod
			row['Pod'] = pod

			#get queried columns
			self._fill_in_queried_cells(row, pod, response_dict)

			row['CPU Requests %'] = self._get_percent(row['CPU Usage'], row['CPU Requests'])
			row['CPU Limits %'] = self._get_percent(row['CPU Usage'], row['CPU Limits'])

			#add row to database
			df.loc[i] = row 
			i += 1

		return df


	#returns a filtered version of json data as a list of dictionaries containing pod, node, and value
	def _parse_json_data(self, json_data):
		res_list = get_result_list(json_data)
		parsed_data = []
		for data_dict in res_list:
			pod = data_dict['metric']['pod']
			node = data_dict['metric']['node']
			value = data_dict['value'][1] #data_dict['value'][0] is the timestamp
			assembled_data = {'node':node, 'pod':pod, 'value':value}
			parsed_data.append(assembled_data)
		return parsed_data

	#calculate the percentages manually to avoid unnecessary querying
	def _get_percent(self, numerator, divisor):
		#Handle None's
		if numerator == None or divisor == None:
			return None

		#if both have values, calculate percent
		return clean_round(float(numerator)/float(divisor)*100)

	#return a dataframe of pods, nodes, and values for a given json_data for a column in a table (e.g. CPUQuota: CPU usage) 
	def _generate_df(self, col_title, raw_json_data):
		#parse data and initialize dataframe
		parsed_data = self._parse_json_data(raw_json_data)
		df = pd.DataFrame(columns = ['Node', 'Pod', col_title])
		#fill in dataframe
		for datapoint in parsed_data:
			#each triplet is a dictionary with node (str), pod (str), values (list)
			node = datapoint['node']
			pod = datapoint['pod']
			value = datapoint['value']
			#add a row to the end of the dataframe containing a node, pod, and value
			df.loc[len(df.index)] = [node, pod, value]
		return df

	#make sure each query returns more than one value. This means it has more than one node.
	def check_success(self):
		for query_title, query in self.queries.items():
			queried_data = query_api_site(query)
			if len(get_result_list(queried_data)) > 0:
				print(colored(query_title, "green"), "\n")
				print(self._generate_df(query_title, queried_data))
				print("\n\n")
			else:
				print(colored(query_title, "red"), "\n")
				pprint(queried_data)
				print("\n\n")

		for query_title, query in self.partial_queries.items():
			queried_data = query_api_site(query)
			if len(get_result_list(queried_data)) > 0:
				print(colored(query_title, "green"), "\n")
				print(self._generate_df(query_title, queried_data))
				print("\n\n")
			else:
				print(colored(query_title, "red"), "\n")
				pprint(queried_data)
				print("\n\n")