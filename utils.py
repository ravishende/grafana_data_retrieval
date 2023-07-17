import requests
import json

def query_select():
	queries = [
		"rate(node_cpu_seconds_total[2m])",	#Per-second rate of increase, averaged over last 2 minutes
		# "irate(node_cpu_seconds_total[1m])", #Per-second rate of increase, calculated over last two samples in a 1 minute window
		# "increase(node_cpu_seconds_total[5h])", #Absolute increase over last 5 hours
		
		#"", #CPU Utilisation (from requests) 
		#"", #Memory Utilisation (from requests)
		#"" #Memory Utilisation (from limits)
		
	]
	return queries

def check_queries():
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	queries_list = query_select()
	for i in range(0,len(queries_list)-1):
		endpoint = f'query?query={queries_list[i]}'
		full_url = base_url + endpoint
		data = requests.get(full_url)

		with open(f'queries_test_{i}.json',"w") as file:
			json.dump(data.json(),file)


def query_api_site():
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	query = 'rate(node_cpu_seconds_total[1m])'
	endpoint = f'query?query={query}'
	full_url = base_url + endpoint
	cpu_data = requests.get(full_url)
	return cpu_data

def write_json(data):
	with open("webscrape.json","w") as file:
		json.dump(data.json(),file)

def read_json():
	with open("webscrape.json","r") as file:
		data = json.load(file)
	return data

"""
The following example expression returns the per-second rate of HTTP requests 
as measured over the last 5 minutes, 
per time series in the range vector:

rate(http_requests_total{job="api-server"}[5m])

rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])

rate()

container_cpu_usage_seconds_total


time = 3h
namespace "wifire-quicfire"











"""