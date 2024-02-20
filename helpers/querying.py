import requests
import json
from termcolor import colored



# Use url and a given query to request data from the website
def query_data(query, handle_fail=True):
    # handle_fail will re request the api if no response from query. Set to true by default
    # there is a bug with the api itself where every fifth request comes back with no data,
    # this parameter set to True will re request to deal with that
    # It is highly recommended that handle_fail is always set to True.

    # set up url
    base_url = "https://thanos.nrp-nautilus.io/api/v1/"
    endpoint = f"query?query={query}"
    full_url = base_url + endpoint
    # query database
    queried_data = requests.get(full_url).json()

    # re-request data if it comes back with no value
    if handle_fail:
        try:
            res_list = queried_data['data']['result']
            if len(res_list) == 0:
                queried_data = requests.get(full_url).json()
        except KeyError:
            print(f'\n\nqueried_data is\n{colored(queried_data,"red")}\n')
            raise TypeError(f'\n\nBad query string: {query}\n\n')

    return queried_data['data']['result']


# Use url and a given query and time_filter to request data for a graph from the api
# Different function from query_data() to avoid confusion with querying single data points and tables vs graphs
def query_data_for_graph(query, time_filter, handle_fail=True):
    # handle_fail will re request the api if it gets no response from your query. Set to true by default
    # there is a bug with the api itself where every fifth request comes back with no data,
    # this parameter set to True will re request to deal with that
    # It is highly recommended that handle_fail is always set to True.

    # set up url
    base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
    endpoint = f'query_range?query={query}&{time_filter}'
    full_url = base_url + endpoint
    # query database
    queried_data = requests.get(full_url).json()

    # re-request data if it comes back with no value
    if (handle_fail):
        try:
            res_list = queried_data['data']['result']
            if (len(res_list) == 0):
                queried_data = requests.get(full_url).json()
        except KeyError:
            print(f'\n\nqueried_data is\n{colored(queried_data,"red")}\n')
            raise TypeError(f'\n\nBad query string: \n{full_url}\n\n')

    return queried_data['data']['result']


# writes json data to a file
def write_json(file_name, data):
    with open(file_name, 'w') as file:
        json.dump(data, file)
    file.close()


# reads json data from a file
def read_json(file_name):
    with open(file_name, 'r') as file:
        data = json.load(file)
    file.close()
    return data
