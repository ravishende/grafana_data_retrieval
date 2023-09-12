from inputs import NAMESPACE
from utils import write_json, query_api_site, get_result_list
from pprint import pprint

duration = '7h'
# query for all metrics with node somewhere in the name
query = '{__name__ =~".*node.*", namespace="' + NAMESPACE + '"}[' + duration + ']'
queried_data = query_api_site(query)

# put all unique names in a list
names_list = []
res_list = get_result_list(queried_data)
for dictionary in res_list:
    for title in dictionary.keys():
        if title == 'metric':
            name = dictionary['metric']['__name__']
            if name not in names_list:
                names_list.append(name)

# print names_list and write it to a file
pprint(names_list)
write_json("node_names.txt", names_list)
