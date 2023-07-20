import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd
from cpu_quota import *
from mem_quota import *
from network_usage import *

column_names=["Pod","CPU Usage","CPU Requests",'CPU Requests %', 'CPU Limits']
data = [ [None, None, None, None, None]]

cpu_df = pd.DataFrame(columns=column_names)
mem_df = pd.DataFrame(columns=column_names)

pods_l = get_pods_list()

cpu_quota = CPUQuota()
mem_quota = MemQuota()
for p in pods_l:
    a = cpu_quota.get_table_row(p)
    cpu_df = cpu_df.append(a, ignore_index = True)
    b = mem_quota.get_table_row(p)
    mem_df = mem_df.append(b, ignore_index=True)


print(cpu_df)
print("\n\n")
print(mem_df)
print("\n\n")
print_query_values()
print("\n\n\n")

print("Total API pings: ",get_query_count())
