from utils import *
from inputs import *
import pandas as pd
from pprint import pprint
from rich import print as printc


class DataTable():
	def __init__(self, namespace=NAMESPACE, duration = DEFAULT_DURATION, round_to=ROUND_TABLES_TO):
		self.namespace = namespace
		self.duration = duration
		self.round_to = round_to
	
	#get a list of all the pods in a table
	def _get_pods(self, response_dict):
		pods = []
		
		#loop through columns
		for column in response_dict.values():
			#loop through pods and values in a given columns
			for item in column: 
				if item['metric']['pod'] not in pods:
					pods.append(item['metric']['pod'])

		return pods

	# def _get_pods_from_json(self, response_dict):
	# 	pods = []
		
	# 	#loop through columns
	# 	for col_title, column in response_dict.items():
	# 		#loop through pods and values in a given columns
	# 		for item in get_result_list(column): 
	# 			if item['metric']['pod'] not in pods:
	# 				pods.append(item['metric']['pod'])

	# 	return pods

	#calculate the percentages manually to avoid unnecessary querying
	def _get_percent(self, numerator, divisor):
		#Handle None's
		if numerator == None or divisor == None:
			return None

		#if both have values, calculate percent
		return clean_round(float(numerator)/float(divisor)*100)


	#update the cells of the given row meant for storing the values retrieved from the query list that generates response_dict
	def _fill_in_queried_cells(self, row, pod, response_dict):
		for col_title, column in response_dict.items():
			
			#(re)set found_pod
			found_pod = False

			#loop through pods and values in a given column
			for pod_value_pair in column: 
				#find the value associated with our current pod
				if(pod_value_pair['metric']['pod'] == pod):
					#update row with new value rounded to 5 decimal places
					row[col_title] = clean_round(pod_value_pair['value'][1], self.round_to) 
					found_pod = True
					break

			#if the pod was not in the column, set the row's cell to be none
			if not found_pod:
				row[col_title] = None


	#returns a table with data from cpu_quota
	def cpu_quota(self, as_json_data=False):
		#dictionary storing all queries besides percentages and pods
		queries_dict = {
			'CPU Usage':'sum by(pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"})',
			'CPU Requests':'sum by(pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="", namespace="' + NAMESPACE + '"})',
			'CPU Limits':'sum by(pod) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="", namespace="' + NAMESPACE + '"})'
		}

		#create a final dictionary for storing columns and their titles
		col_names = ["Pod", "CPU Usage", "CPU Requests", "CPU Requests %", "CPU Limits", "CPU Limits %"]
		response_dict = {}

		#store json data from querying the api
		for col_title, query in queries_dict.items():
			# response_dict[col_title] = get_result_list(query_api_site(query))
			queried_data = query_api_site(query)
			printc(col_title, "\n", queried_data, "\n")
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


	#returns a data table with data from mem_quota
	def mem_quota(self):
		#dictionary storing all queries besides percentages and pods
		queries_dict = {
			"Memory Usage":'sum by(pod) (container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '", container!="", image!=""})', 	
			"Memory Requests":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="' + NAMESPACE + '"})', 
			"Memory Limits":'sum by(pod) (cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="' + NAMESPACE + '"})', 
			"Memory Usage (RSS)":'sum by(pod) (container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!=""})', 
			"Memory Usage (Cache)":'sum by(pod) (container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!=""})', 
			"Memory Usage":'sum by(pod) (container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!=""})'
		}
		
		#create a final dictionary for storing columns and their titles
		col_names = ["Pod", "Memory Usage","Memory Requests",  "Memory Requests %",  "Memory Limits", "Memory Limits %", "Memory Usage (RSS)", "Memory Usage (Cache)"]
		response_dict = {}

		#store json data from querying the api
		for col_title, query in queries_dict.items():
			# print(f'{colored("Mem_quota", "yellow")}: querying, {colored(col_title, "green")}')
			response_dict[col_title] = get_result_list(query_api_site(query))

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

			#get percent columns
			row['Memory Requests %'] = self._get_percent(row['Memory Usage'], row['Memory Requests'])
			row['Memory Limits %'] = self._get_percent(row['Memory Usage'], row['Memory Limits'])

			#add row to database
			df.loc[i] = row 
			i += 1

		return df


	#current network usage requires a duration. This duration has a default value, but should generally be passed in.
	def network_usage(self):
		#assemble queries for the given pod
		queries_dict = {
			"Current Receive Bandwidth":'sum by(pod) (irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + self.duration + ']))',
			"Current Transmit Bandwidth":'sum by(pod) (irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + self.duration + ']))',
			"Rate of Received Packets":'sum by(pod) (irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + self.duration + ']))',
			"Rate of Transmitted Packets":'sum by(pod) (irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + self.duration + ']))',
			"Rate of Received Packets Dropped":'sum by(pod) (irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + self.duration + ']))',	
			"Rate of Transmitted Packets Dropped":'sum by(pod) (irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + self.duration + ']))'
		}

		#create a final dictionary for storing columns and their titles
		col_names = ["Pod","Current Receive Bandwidth", "Current Transmit Bandwidth", "Rate of Received Packets", "Rate of Transmitted Packets", "Rate of Received Packets Dropped", "Rate of Transmitted Packets Dropped"]
		response_dict = {}

		#store json data from querying the api
		for col_title, query in queries_dict.items():
			response_dict[col_title] = get_result_list(query_api_site(query))

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

			#add row to database
			df.loc[i] = row 
			i += 1

		return df

	def storage_io(self, duration=DEFAULT_DURATION):
		#assemble queries for the given pod
		queries_dict = {
			'IOPS(Reads)':'sum by(pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']))',
			'IOPS(Writes)':'sum by(pod) (rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']))',
			#query that doesn't work but can probably be calculated with Read + Write
			# 'IOPS(Reads + Writes)':'sum by(pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']))',
			'Throughput(Read)':'sum by(pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']))',
			'Throughput(Write)':'sum by(pod) (rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']))'
			#query that doesn't work but can probably be calculated with Read + Write
			# 'Throughput(Read + Write)':'sum by(pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", namespace="' + NAMESPACE + '"}[' + duration + ']))'
		}

		#create a final dictionary for storing columns and their titles
		col_names = ["Pod","IOPS(Reads)", "IOPS(Writes)", "IOPS(Reads + Writes)," "Throughput(Read)", "Throughput(Write)", "Throughput(Read + Write)"]
		response_dict = {}


		for col_title, query in queries_dict.items():
			response_dict[col_title] = get_result_list(query_api_site(query))

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

			row['IOPS(Reads + Writes)'] = [read+write for read, write in zip(row['IOPS(Reads)'], row['IOPS(Writes)'])]
			row['Throughput(Read + Write)'] = [read+write for read, write in zip(row['Throuput(Read)'], row['Throughput(Write)'])]

			#add row to database
			df.loc[i] = row 
			i += 1

		return df
