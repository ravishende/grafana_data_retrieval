import json
import requests
from bs4 import BeautifulSoup

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

data = query_api_site()
write_json(data)


# url = "https://grafana.nrp-nautilus.io/d/85a562078cdf77779eaa1add43ccec1e/kubernetes-compute-resources-namespace-pods?orgId=1&var-datasource=thanos&var-cluster=&var-namespace=wifire&from=1756419957757&to=1806913309834"
# all_data = requests.get(url)

# s = BeautifulSoup(all_data.content, "html.parser")
# print(s)
# print("successfully requested url")

# res = s.find(id="containername")
# title = s.find("h2",class_="title is-5")
# print(title[0].text)
