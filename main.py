import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd
from cpu_quota import *
from mem_quota import *

column_names=["Pod","CPU Usage","CPU Requests",'CPU Requests %', 'CPU Limits']
data = [ [None, None, None, None, None]]

df = pd.DataFrame(columns=column_names)

def get_pods_list():
	data = query_api_site('rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[3h])')
	try:
		pods = data["data"]["result"]
		pods_list = []
		for pod in pods:
			pods_list.append(pod["metric"]["pod"])
	except KeyError as e:
		return ["Error retrieving pods"]
	return pods_list

pods_l = get_pods_list()


for p in pods_l:
	cpu_quota = CPUQuota()
	a = cpu_quota.get_table_row(p)
	# df = df.append(a, ignore_index = True)


# print(df)
print("\n\n\n")