# For getting information from queries retrieved by node_metrics_retrieval.py
# To determine if this is information that should be saved or investigated further
import sys
import os
# get set up to be able to import files from parent directory (grafana_data_retrieval)
# utils.py and inputs.py not in this current directory and instead in the parent
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath("header.py"))
parent = os.path.dirname(current)
sys.path.append(parent)
from inputs import NAMESPACE
from utils import read_json, query_api_site, get_result_list, print_sub_title, print_title
from pprint import pprint
from termcolor import colored

duration = '15m'

# read node_names.txt (updated by node_metrics_retrieval.txt) to get a list of metrics
metrics_list = read_json("node_names.txt")
queries_list = []

# assemble queries from the names_list
for metric in metrics_list:
    query_str = f'{str(metric)}{{namespace="{NAMESPACE}"}}[{duration}]'
    queries_list.append(query_str)

# get the data from the queries and print it.
print_title("Queries Data")
for i in range(len(queries_list)):
    result_list = get_result_list(query_api_site(queries_list[i]))
    print_sub_title(metrics_list[i])
    if len(result_list) == 0:
        print(colored("No data", "red"))
        continue
    pprint(result_list)
print("\n\n")