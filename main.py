import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd
from data_table import DataTable
from network_usage import *


pd.set_option('display.max_columns', 8)

# pods_l = get_pods_list()

#generate pickle

table_retreiver = DataTable()
cpu_df = table_retreiver.cpu_quota()
mem_df = table_retreiver.mem_quota()
network_df = table_retreiver.network_usage("1h")


# mem_df.to_pickle("mem_df.pkl")
# cpu_df.to_pickle("cpu_df.pkl")

# mem_df = pd.read_pickle("mem_df.pkl")  
# cpu_df = pd.read_pickle("cpu_df.pkl")

# print(cpu_df.to_string())
print("\n\n\n\n")
print_query_values()
print(mem_df.to_string())
print("\n\n\n\n")

print(cpu_df.to_string())
print("\n\n\n\n")

print(network_df)
print("\n\n\n\n")



print("Total API pings: ",get_query_count())

print("\n")
query_api_site('sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="wifire-quicfire"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="", namespace="wifire-quicfire", resource="cpu"})',handle_fail=True)
print("\n")


