#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
from inputs import *
from decimal import *
from datetime import datetime, timedelta
from termcolor import cprint, colored
from pprint import pprint

#reset total query count
QUERY_COUNT = 0

#retrieves information from the 4 panels under headlines (cpu and memory utilisation data)
header_queries = {
	'CPU Utilisation (from requests)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
	'CPU Utilisation (from limits)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="cpu"})',
	'Memory Utilisation (from requests)': 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!="", image!=""}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="memory"})',
	'Memory Utilisation (from limits)': 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!="", image!=""}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="' + NAMESPACE + '", resource="memory"})',
}

#generates a list of all pods being shown
def get_pods_list():
	data = query_api_site('rate(container_cpu_usage_seconds_total{namespace="' + NAMESPACE + '"}[3h])')
	try:
		pods = data["data"]["result"]
		pods_list = []
		for pod in pods:
			pods_list.append(pod["metric"]["pod"])
	except KeyError as e:
		return ["Error retrieving pods"]
	return pods_list


#generates a dictionary of the 4 headers and their values
def find_header_values(query_dict=header_queries):
	query_values = {}
	for query_title, query in query_dict.items():
		#get data
		res_list = get_result_list(query_api_site(query))
		#handle if there isn't data
		if len(res_list) == 0:
			query_values[query_title] = "No data"
		else:
			#add data to dictionary
			query_values[query_title] = clean_round(float(res_list[0]['value'][1]),3)

	return query_values


#print the values of the 4 header panels
def print_header_values(as_percentages=False):
	#get query values
	query_values = find_header_values()

	#print out values
	for (query_title, value) in query_values.items():
		
		#check if value is empty
		if (str(value)[0] == "N"):
			print(f'{query_title}: \n\t{colored(value,"red")}')
		else:
			# print the percentage sign if requested
			if(as_percentages):
				print(f'{query_title}: \n\t{colored(round(value*100, 1), "green")}{colored("%", "green")}')
			else:
				print(f'{query_title}: \n\t{colored(value,"green")}')


# Use url and a given query to request data from the website
def query_api_site(query, handle_fail=True):
	# handle fail will re request the api if it gets no response from your query. Set to true by default
	# there is a bug with the api itself where every fifth request comes back with no data, 
	# this parameter set to True will re request to deal with that	
	
	global QUERY_COUNT
	#set up url
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	endpoint = f'query?query={query}'
	full_url = base_url + endpoint
	#query database
	queried_data = requests.get(full_url).json()
	QUERY_COUNT += 1

	#re-request data if it comes back with no value
	if handle_fail:
		try:
			res_list = get_result_list(queried_data)
			if len(res_list) == 0:
				queried_data = requests.get(full_url).json()
		except KeyError:
			print(f'\n\nqueried_data is\n{colored(queried_data,"red")}\n')
			raise TypeError(f'\n\nBad query string: {query}\n\n')

	return queried_data


# Use url and a given query and time_filter to request data for a graph from the api
# Different function from query_api_site() to avoid confusion with querying single data points and tables vs graphs
def query_api_site_for_graph(query, time_filter, handle_fail=True):
	# handle fail will re request the api if it gets no response from your query. Set to true by default
	# there is a bug with the api itself where every fifth request comes back with no data, 
	# this parameter set to True will re request to deal with that	

	global QUERY_COUNT
	#set up url
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	endpoint = f'query_range?query={query}&{time_filter}'
	full_url = base_url + endpoint
	#query database
	queried_data = requests.get(full_url).json()
	QUERY_COUNT += 1

	#re-request data if it comes back with no value
	if (handle_fail):
		try:
			res_list = get_result_list(queried_data)
			if (len(res_list) == 0):
				queried_data = requests.get(full_url).json()
		except KeyError:
			print(f'\n\nqueried_data is\n{colored(queried_data,"red")}\n')
			raise TypeError (f'\n\nBad query string: \n{full_url}\n\n')

	return queried_data



#given json data from querying the api, retrieve the result of the query as a list of two floats
def get_result_list(api_response):
	return api_response['data']['result']


#retrieves global variable for total number of querries since the start of the program
def get_query_count():
	global QUERY_COUNT
	return QUERY_COUNT

#given a number and X decimal places, if there are fewer than X decimal places, return it, otherwise round to X decimal places 
def clean_round(number, place=DEFAULT_ROUND_TO):
	#find the number of decimal places kept in the string
	current_places = str(number)[::-1].find(".")
	#if it has fewer places than the specified number, return it, otherwise round it to the specified number of places
	if(current_places > place):
		return float(round(Decimal(number), place))
	return number

#writes json data to a file
def write_json(data):
	with open('e.json', 'w') as file:
		json.dump(data.json(), file)


#reads json data from a file
def read_json():
	with open('scrape.json', 'r') as file:
		data = json.load(file)
	return data
