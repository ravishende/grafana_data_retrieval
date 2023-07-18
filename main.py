import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd


column_names=["CPU Usage","CPU Requests",'CPU Requests %', 'CPU Limits']
data = [ [None, None, None, None]]

df = pd.DataFrame(data, columns=column_names)

def get_cpu_usage(pod):
	cpu_usage = query_api_site('sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire", pod="'+pod+'"})')
	cpu_usage = cpu_usage.json()['data']['result']
	
	# if error retrieving from api, ping again recursively
	if (len(cpu_usage) == 0):

		return get_cpu_usage(pod)

	return float(cpu_usage[0]['value'][1])

cpu = round(get_cpu_usage('bp3d-rabbitmq-5845c99598-99rcn'),2)

df["CPU Usage"][0] = cpu

# row = {'CPU Usage':cpu, 'CPU Requests':0, 'CPU Requests %': 0, 'CPU Limits': 0}
# df = df.append(row, ignore_index = True)
print(df)
print("\n\n\n")