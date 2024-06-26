# This file is for analyzing all potential information that can be queried regarding nodes.
# Not necessary in the general program. More of a side enhancement.
from pprint import pprint
import sys
import os
# get set up to be able to import files from parent directory (grafana_data_retrieval)
# utils.py and inputs.py not in this current directory and instead in the parent
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from inputs import NAMESPACE
from helpers.querying import write_json, query_data


# query for all metrics with node somewhere in the name
duration = '1w'
query = '{__name__ =~".*node.*", namespace="' + NAMESPACE + '"}[' + duration + ']'
res_list = query_data(query)

# put all unique names in a list
names_list = []
for dictionary in res_list:
    for title in dictionary.keys():
        if title == 'metric':
            name = dictionary['metric']['__name__']
            if name not in names_list:
                names_list.append(name)

# print names_list and write it to a file
pprint(names_list)
write_json("node_names.txt", names_list)
