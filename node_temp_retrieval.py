import requests
import json
import re
from utils import write_json, query_api_site
from pprint import pprint

# base_url = "https://thanos.nrp-nautilus.io/api/v1/"
# endpoint = "name/values"
# full_url = base_url + endpoint
# query database
# queried_data = requests.get(full_url).json()

# query = 'group by(__name__) ({__name__=".*node.*"})' #trying to use regex to filter for names with 'node' in it
query = 'group by(__name__) ({__name__!=""})'
queried_data = query_api_site(query)

pprint(queried_data)
# write_json(queried_data)
file = open("node_temp_data.txt", "a")
file.write(queried_data)
