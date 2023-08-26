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
		self.cpu_quota = pd.DataFrame(columns = ["Pod", "Node", "CPU Usage", "CPU Requests", "CPU Requests %", "CPU Limits", "CPU Limits %"])
		self.mem_quota = pd.DataFrame(columns = ["Pod", "Node", "Memory Usage","Memory Requests",  "Memory Requests %",  "Memory Limits", "Memory Limits %", "Memory Usage (RSS)", "Memory Usage (Cache)"])
		self.network_usage = pd.DataFrame(columns = ["Pod", "Node", "Current Receive Bandwidth", "Current Transmit Bandwidth", "Rate of Received Packets", "Rate of Transmitted Packets", "Rate of Received Packets Dropped", "Rate of Transmitted Packets Dropped"])
		self.storage_io = pd.DataFrame(columns = ["Pod", "Node", "IOPS(Reads)", "IOPS(Writes)", "IOPS(Reads + Writes)", "Throughput(Read)", "Throughput(Write)", "Throughput(Read + Write)"])
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


	'''
	_______________________________________________________________________________
	
	Private Methods
	_______________________________________________________________________________

	'''


	#return a dataframe of pods, nodes, and values for a given json_data for a column in a table (e.g. CPUQuota: CPU usage) 
	def _generate_df(self, col_title, raw_json_data):
		#initialize dataframe and filter json data
		df = pd.DataFrame(columns = ['Node', 'Pod', col_title])
		res_list = get_result_list(raw_json_data)
		#fill in dataframe
		for datapoint in res_list:
			#each datapoint in parsed_data is a dictionary with node (str), pod (str), (int)
			node = datapoint['metric']['node']
			pod = datapoint['metric']['pod']
			value = datapoint['value'][1] #data_point['value'][0] is the timestamp
			#add a row to the end of the dataframe containing a node, pod, and value
			df.loc[len(df.index)] = [node, pod, value]
		return df


	#returns an updated dataframe by filling in data queried from the columns in a passed in dataframe
	def _fill_df_by_queries(self, table_df, queries=None):
		if queries == None:
			queries = self.queries
		#update the columns of the database with 
		for col_title in table_df.columns:
			#get the corresponding query for each column
			query = queries.get(col_title)
			if query == None:
				continue
			#update the table with the new column information
			queried_data = query_api_site(query)
			new_df = self._generate_df(col_title, queried_data)
			# table_df.merge(new_df, how='left')
			if len(table_df.index) > 0:
				final_col = new_df.columns[-1]
				table_df[final_col] = new_df[final_col]
			else:
				for column in new_df.columns:
					table_df[column] = new_df[column]
		return table_df

	
	'''
	_______________________________________________________________________________
	
	Public Methods
	_______________________________________________________________________________

	'''
	
	def get_cpu_quota(self):
		#check if the table has been filled in
		if len(self.cpu_quota.index) > 0:
			return self.cpu_quota

		#if not, fill in the table then return it
		self.cpu_quota = self._fill_df_by_queries(self.cpu_quota)
		#calculate each percent column by dividing the two columns responsible for it then multiplying by 100
		self.cpu_quota['CPU Requests %'] = self.cpu_quota['CPU Usage'].astype(float).div(self.cpu_quota['CPU Requests'].astype(float)).multiply(100)
		self.cpu_quota['CPU Limits %'] = self.cpu_quota['CPU Usage'].astype(float).div(self.cpu_quota['CPU Limits'].astype(float)).multiply(100)
		return self.cpu_quota


	def get_mem_quota(self):
		#check if the table has been filled in
		if len(self.mem_quota.index) > 0:
			return self.mem_quota

		#if not, fill in the table then return it
		self.mem_quota = self._fill_df_by_queries(self.mem_quota)
		#calculate each percent column by dividing the two columns responsible for it
		self.mem_quota['Memory Requests %'] = self.mem_quota['Memory Usage'].astype(float).div(self.mem_quota['Memory Requests'].astype(float)).multiply(100)
		self.mem_quota['Memory Limits %'] = self.mem_quota['Memory Usage'].astype(float).div(self.mem_quota['Memory Limits'].astype(float)).multiply(100)
		return self.mem_quota


	def get_network_usage(self):
		#check if the table has been filled in
		if len(self.network_usage.index) > 0:
			return self.network_usage

		#if not, fill in the table then return it
		self.network_usage = self._fill_df_by_queries(self.network_usage)
		return self.network_usage


	def get_storage_io(self):
		#check if the table has been filled in
		if len(self.storage_io.index) > 0:
			return self.storage_io

		#if not, fill in the table then return it
		self.storage_io = self._fill_df_by_queries(self.storage_io, queries=self.partial_queries)
		#calculate each sum column by adding the two columns responsible for it
		self.storage_io['IOPS(Reads + Writes)'] = self.storage_io['IOPS(Reads)'].astype(float) + self.storage_io['IOPS(Writes)'].astype(float)
		self.storage_io['Throughput(Read + Write)'] = self.storage_io['Throughput(Read)'].astype(float) + self.storage_io['Throughput(Write)'].astype(float)
		return self.storage_io



	def get_tables_dict(self):
		tables_dict = {
			'CPU Quota':self.get_cpu_quota(),
			'Memory Quota':self.get_mem_quota(),
			'Current Network Usage':self.get_network_usage(),
			'Current Storage IO':self.get_storage_io()
		}
		return tables_dict


	def print_tables(self):
		tables_dict = self.get_tables_dict()
		for title, table in tables_dict.items():
			print("\n\n______________________________________________________________________________\n")
			print("            ", colored(title, "green"))
			print("______________________________________________________________________________\n")
			if len(table.index) > 0:
				print(table)
			else:
				print("        No Data")
			print("\n\n")





