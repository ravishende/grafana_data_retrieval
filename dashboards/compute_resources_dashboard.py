# autopep8: off
import sys
import os
sys.path.append("../grafana_data_retrieval")  # Adjust the path to go up one level
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# pylint: disable=wrong-import-position
from graphs import Graphs
from graph_visualization import display_graphs
from helpers.printing import print_dataframe_dict
# autopep8: on

# ==========================================================================================
# NOTE:
# This is the dashboard for the following Grafana page:
# https://grafana.nrp-nautilus.io/d/Tf9PkuSik/k8s-nvidia-gpu-node?orgId=1&refresh=15m&var-interval=1m&var-host=node-2-3.sdsc.optiputer.net
# ==========================================================================================

# display settings
GET_GRAPHS_AS_SINGLE_DF = False
VISUALIZE_GRAPHS = False

HOST = "node-2-3.sdsc.optiputer.net"
queries = {
    'GPU Utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + HOST + '"}',
    'Memory Copy Utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + HOST + '"}',
    'Power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + HOST + '"}',
    'Temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + HOST + '"}',
    'Fan Speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + HOST + '"}'
}

# get graphs class
graphs_class = Graphs()
# get dict containing all queried graphs
graphs_dict = graphs_class.get_graphs_from_queries(
    queries, sum_by=["namespace", "pod"])


# logic for displaying graphs
if not GET_GRAPHS_AS_SINGLE_DF:
    # print multiple graphs
    print_dataframe_dict(graphs_dict)
else:
    # print graphs as single dataframe
    graphs_df = graphs_class.get_graphs_as_one_df(
        graphs_dict, sum_by=["namespace", "pod"])
    print(graphs_df)

if VISUALIZE_GRAPHS:
    # display graphs in another window
    display_graphs(graphs_dict, sum_by=["namespace", "pod"])
