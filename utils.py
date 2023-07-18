#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
from termcolor import cprint, colored
from pprint import pprint

ts = '[1h]'
#TODO: handle time inputs for data collection


#retrieves information from the 4 panels under headlines (cpu and memory utilisation data)
QUERIES = {
    'CPU Utilisation (from requests)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',
    'CPU Utilisation (from limits)': 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',
    'Memory Utilisation (from requests)': 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})',
    'Memory Utilisation (from limits)': 'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", image!=""}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="memory"})',
    }

#parses json for numerical data values
def find_query_values():
    query_values = {}

    #generates a new dictionary that holds the data titles and their values
    for (query_title, query) in QUERIES.items():
        api_response = query_api_site(query).json()
        result_list = get_result_list(api_response)

        #there is a bug with the api itself where every fifth request comes back with no data
        if(len(result_list) == 0):
            #the fix to this is simply to regenerate the response if it comes back empty.    
            api_response = query_api_site(query).json()
            query_values[query_title] = get_query_value(get_result_list(api_response))
        else:
            query_values[query_title] = get_query_value(result_list)

    return query_values


#given json data from querying the api, retrieve the result of the query as a list of two floats
def get_result_list(api_response):
    return api_response['data']['result']

#given a two element result list, select the second element and make it a usable float.
def get_query_value(result_list):
    return round(float(result_list[0]['value'][1]),3)


#print the values of the 4 header panels
def print_query_values(as_percentages=False):
    #get query values
    query_values = find_query_values()

    #print out values
    for (query_title, value) in query_values.items():
        
        #check if value is empty
        if (str(value)[0] == "n"):
        	print(f'{query_title}: \n\t{colored(value,"red")}')
        else:
            # print the percentage sign if requested
            if(as_percentages):
                print(f'{query_title}: \n\t{colored(round(value*100, 1), "green")}{colored("%", "green")}')
            else:
                print(f'{query_title}: \n\t{colored(value,"green")}')


#use url and a given query to request data from the website
def query_api_site(query=QUERIES['CPU Utilisation (from requests)']):
    base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
    endpoint = f'query?query={query}'
    full_url = base_url + endpoint
    cpu_data = requests.get(full_url)
    return cpu_data



def write_json(data):
    with open('e.json', 'w') as file:
        json.dump(data.json(), file)


def read_json():
    with open('scrape.json', 'r') as file:
        data = json.load(file)
    return data



