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
        try:
            query_values[query_title] = \
                round(float(query_api_site(query).json()[
                    'data']['result'][0]['value'][1]),3)

        #deal with improper/no data
        except KeyError:
            query_values[query_title] = 'no data/failed response'
        except IndexError:
            query_values[query_title] = 'no data/failed response'

    return query_values


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

#TODO: delete this later; it is just for developing and testing
def json_print(query = 'rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])'):
    query_data = query_api_site(query).json()
    pprint(query_data)






def write_json(data):
    with open('e.json', 'w') as file:
        json.dump(data.json(), file)


def read_json():
    with open('scrape.json', 'r') as file:
        data = json.load(file)
    return data



