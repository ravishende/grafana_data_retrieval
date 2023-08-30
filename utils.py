#!/usr/bin/python3
# -*- coding: utf-8 -*-
import requests
import json
import re
from termcolor import colored

# reset total query count
QUERY_COUNT = 0


# retrieves global variable for total number of querries
# since the start of the program
def get_query_count():
    global QUERY_COUNT
    return QUERY_COUNT


# Use url and a given query to request data from the website
def query_api_site(query, handle_fail=True):
    # handle fail will re request the api if no response from query. Set to true by default
    # there is a bug with the api itself where every fifth request comes back with no data,
    # this parameter set to True will re request to deal with that

    global QUERY_COUNT
    # set up url
    base_url = "https://thanos.nrp-nautilus.io/api/v1/"
    endpoint = f"query?query={query}"
    full_url = base_url + endpoint
    # query database
    queried_data = requests.get(full_url).json()
    QUERY_COUNT += 1

    # re-request data if it comes back with no value
    if handle_fail:
        try:
            res_list = get_result_list(queried_data)
            if len(res_list) == 0:
                queried_data = requests.get(full_url).json()
        except KeyError:
            print(f'\n\nqueried_data is\n{colored(queried_data,"red")}\n')
            raise TypeError(f'\n\nBad query string: {query}\n\n')

    return queried_data


# Use url and a given query and time_filter to request data for a graph from the api
# Different function from query_api_site() to avoid confusion with querying single data points and tables vs graphs
def query_api_site_for_graph(query, time_filter, handle_fail=True):
    # handle fail will re request the api if it gets no response from your query. Set to true by default
    # there is a bug with the api itself where every fifth request comes back with no data,
    # this parameter set to True will re request to deal with that

    global QUERY_COUNT
    # set up url
    base_url = 'https://thanos.nrp-nautilus.io/api/v1/'
    endpoint = f'query_range?query={query}&{time_filter}'
    full_url = base_url + endpoint
    # query database
    queried_data = requests.get(full_url).json()
    QUERY_COUNT += 1

    # re-request data if it comes back with no value
    if (handle_fail):
        try:
            res_list = get_result_list(queried_data)
            if (len(res_list) == 0):
                queried_data = requests.get(full_url).json()
        except KeyError:
            print(f'\n\nqueried_data is\n{colored(queried_data,"red")}\n')
            raise TypeError(f'\n\nBad query string: \n{full_url}\n\n')

    return queried_data


# given json data from querying the api, retrieve the result
# of the query as a list of two floats
def get_result_list(api_response):
    return api_response['data']['result']


# given a string in the form 5w3d6h30m5s, save the times to a dict accesible
# by the unit as their key. The int times can be any length (500m160s is allowed)
# works given as many or few of the time units. (e.g. 12h also works and sets everything but h to None)
def get_time_dict_from_str(time_str):
    # define regex pattern (groups by optional int+unit but only keeps the int)
    pattern = "(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
    feedback = re.search(pattern, time_str)

    # save time variables (if not in time_str they will be set to None)
    w, d, h, m, s = feedback.groups()
    # put time variables into a dictionary
    time_dict = {
        'weeks': w,
        'days': d,
        'hours': h,
        'minutes': m,
        'seconds': s
    }

    # get rid of null values in time_dict
    time_dict = {unit: float(value) for unit, value in time_dict.items() if value is not None}

    return time_dict

#gets the worker id for a given pod or returns none if it is not a bp3d-worker
def get_worker_id(pod_name):
    worker_title = 'bp3d-worker-k8s-'
    # if pod_name is a bp3d worker, return the ensemble
    title_len = len(worker_title)
    if pod_name[0:title_len] == worker_title:
        return pod_name[title_len:-1]
    # otherwise, return None
    return None

# for every worker pod in a given df, change pod's value to just be the worker id, 
# drop all non-worker pods, then return that new, filtered dataframe
def filter_df_for_workers(dataframe):
    dataframe = dataframe.apply(lambda row: get_worker_id(row["Pod"]))
    dataframe = dataframe.dropna(columns=["Pod"])
    return dataframe


# writes json data to a file
def write_json(data):
    with open('e.json', 'w') as file:
        json.dump(data.json(), file)


# reads json data from a file
def read_json():
    with open('scrape.json', 'r') as file:
        data = json.load(file)
    return data
