from datetime import datetime, timedelta
import shutil
import pandas as pd
from termcolor import colored
import sys
import os
sys.path.append("../grafana_data_retrieval")  # Adjust the path to go up one level
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from graphs import Graphs
from tables import Tables
from graph_visualization import display_graphs
from helpers.printing import print_dataframe_dict, print_title
from helpers.querying import query_data


# display settings
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

# inputs and settings
namespace = "ndp-test"
# namespace = "bvl"  # namespace that often has data - for testing
# for graphs:
time_range = "1h"  # the amount of time that the graph will have data for
timestep = "1m"  # how often datapoints are queried for
# end_time = datetime.now() - timedelta(hours=5)  # uncomment this line and then pass end_time into Graphs(end=end_time) in order to query from x hours ago
# display settings
get_graphs_as_single_df = False
visualize_graphs = False

# This is the dashboard for the following Grafana page: https://grafana.nrp-nautilus.io/d/dRG9q0Ymz/k8s-compute-resources-namespace-gpus?orgId=1&refresh=30s&var-namespace=ndp-test

def combine_dataframes_on_pod(df1, df2):
    # concatenate the dataframes along the columns axis
    combined_df = pd.concat([df1, df2], axis=1)
    # drop one of the duplicate 'Pod' columns
    if 'Pod' in combined_df.columns:
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    return combined_df

def get_datapoint(query):
    result_list = query_data(query)
    if len(result_list) > 0:
        return result_list['value']
    return None

# single cell query
current_gpu_usage_query = 'sum(cDCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"})/count(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"})'
# graph queries
graph_queries_no_sum = {
    'Total GPU usage':'avg_over_time(namespace_gpu_utilization{namespace=~"' + namespace + '"}[5m])',
    'Requested GPUs':'count(DCGM_FI_DEV_GPU_TEMP{namespace=~"' + namespace + '"})'
}
graph_queries_by_pod = {
    "GPUs utilization % by pod":'sum(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod) / count(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod)'
}
# table queries
table_queries_by_pod = {
    'GPU Utilization':'sum(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod) / count(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod)'
}
table_queries_by_pod_model = {
    'GPUs Requested':'count(DCGM_FI_DEV_GPU_TEMP{namespace=~"' + namespace + '"}) by (pod, modelName)'
}

# get necessary classes
graphs_class = Graphs(time_offset=time_range, time_step=timestep)  # if time_offset and time_step are not passed in, default values are used from inputs.py
tables_class = Tables()

# get tables and graphs data
# table_cell_dict = tables_class.get_table_from_queries(table_queries_no_sum, sum_by=None, table_name="Current GPU Usage")``
data_gpu_usage = get_datapoint(current_gpu_usage_query)
table_1 = tables_class.get_table_from_queries(table_queries_by_pod, sum_by='pod')
table_2 = tables_class.get_table_from_queries(table_queries_by_pod_model, sum_by=['pod', 'modelName'])
graphs_dict_no_sum = graphs_class.get_graphs_from_queries(graph_queries_no_sum, sum_by=None)
graphs_dict_by_pod = graphs_class.get_graphs_from_queries(graph_queries_by_pod, sum_by='pod')

# since the Panel Title table has two different sum_by's, it has to be combined from two tables
table_final = combine_dataframes_on_pod(table_2, table_1)
table_dict = {"Panel Title": table_final}

# display Current GPU Usage cell
data_gpu_usage_title = "Current GPU usage"
print_title(data_gpu_usage_title)
if data_gpu_usage is not None:
    print(data_gpu_usage)
else:
    print(colored("No Data", "red"))
print("\n"*2)

# logic for displaying graphs
if not get_graphs_as_single_df:
    # print multiple graphs
    print_dataframe_dict(graphs_dict_no_sum)
    print_dataframe_dict(graphs_dict_by_pod)
else:
    # print graphs as single dataframe
    graphs_df_no_sum = graphs_class.get_graphs_as_one_df(graphs_dict_no_sum, sum_by=None)
    graphs_df_by_pod = graphs_class.get_graphs_as_one_df(graphs_dict_by_pod, sum_by="pod")
    print(graphs_df_no_sum)
    print(graphs_df_by_pod)

if visualize_graphs:
    # display graphs in another window 
    display_graphs(graphs_dict_no_sum, sum_by=None)
    display_graphs(graphs_dict_by_pod, sum_by="pod")

# display table
print_dataframe_dict(table_dict)