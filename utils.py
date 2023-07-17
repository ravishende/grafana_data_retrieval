import requests
import json

def query_api_site():
	base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
	query = 'rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])'
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
