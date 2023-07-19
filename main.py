import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd
from cpu_quota import *

column_names=["CPU Usage","CPU Requests",'CPU Requests %', 'CPU Limits']
data = [ [None, None, None, None]]

df = pd.DataFrame(data, columns=column_names)

M = CPUQuota()
a = M.get_table_row('bp3d-rabbitmq-5845c99598-99rcn')

# cpu = round(get_cpu_usage('bp3d-rabbitmq-5845c99598-99rcn'),2)

# df = cpu

# row = {'CPU Usage':cpu, 'CPU Requests':0, 'CPU Requests %': 0, 'CPU Limits': 0}
df = df.append(a, ignore_index = True)

print(df)
print("\n\n\n")