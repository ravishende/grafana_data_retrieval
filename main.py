import json
import requests
from bs4 import BeautifulSoup
from utils import *
from inputs import *
import pandas as pd
from data_table import DataTable
from termcolor import cprint, colored




pd.set_option('display.max_columns', 8)

table_retreiver = DataTable()
cpu_df = table_retreiver.cpu_quota()
mem_df = table_retreiver.mem_quota()
network_df = table_retreiver.network_usage("1h")

# #generate pickle
# mem_df.to_pickle("mem_df.pkl")
# cpu_df.to_pickle("cpu_df.pkl")

# #read pickle
# mem_df = pd.read_pickle("mem_df.pkl")  
# cpu_df = pd.read_pickle("cpu_df.pkl")

# print(cpu_df.to_string())


#print collected data
print(f'\n\n\n\n{colored("Header:", "cyan")}')
print_header_values()

print(f'\n\n\n\n{colored("CPU Quota:", "cyan")}')
print(cpu_df.to_string())

print(f'\n\n\n\n{colored("Memory Quota:", "cyan")}')
print(mem_df.to_string())

print(f'\n\n\n\n{colored("Current Network Usage:", "cyan")}')
print(network_df)
print("\n\n\n\n")



print("Total API pings: ", get_query_count())

