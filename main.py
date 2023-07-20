import json
import requests
from bs4 import BeautifulSoup
from utils import *
import pandas as pd
from cpu_quota import *
from mem_quota import *
from network_usage import *


pd.set_option('display.max_columns', None)

# pods_l = get_pods_list()

# cpu_quota = CPUQuota()
# mem_quota = MemQuota()

# cpu_df = pd.DataFrame(columns=get_column_names(cpu_quota))
# mem_df = pd.DataFrame(columns=get_column_names(mem_quota))

# for p in pods_l:
#     a = cpu_quota.get_table_row(p)
#     cpu_df = cpu_df.append(a, ignore_index = True)
#     b = mem_quota.get_table_row(p)
#     mem_df = mem_df.append(b, ignore_index=True)


# mem_df.to_pickle("mem_df.pkl")
# cpu_df.to_pickle("cpu_df.pkl")

mem_df = pd.read_pickle("mem_df.pkl")  
cpu_df = pd.read_pickle("cpu_df.pkl")

print(cpu_df.to_string())
print("\n\n\n\n")
print(mem_df.to_string())

# print_query_values()
print("\n\n\n")

print("Total API pings: ",get_query_count())
