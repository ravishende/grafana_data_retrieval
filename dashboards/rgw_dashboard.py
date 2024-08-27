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
# This is the dashboard for the following Grafana page:
# https://grafana.nrp-nautilus.io/d/WaJ_lohMk/ceph-s3?orgId=1&refresh=10s&from=now-1h&to=now
# ==========================================================================================


# display settings
GET_GRAPHS_AS_SINGLE_DF = False
VISUALIZE_GRAPHS = False

# define inputs for querying
DURATION = '1h'
NAMESPACE = "rook"
queries = {
    'Queue length': 'sum by(instance) (ceph_rgw_qlen{namespace="' + NAMESPACE + '"})',
    'RGW Cache Hit': 'sum by(instance) (irate(ceph_rgw_cache_hit{namespace="' + NAMESPACE + '"}[' + DURATION + ']))',
    'RGW Cache Miss': 'sum by(instance) (irate(ceph_rgw_cache_miss{namespace="' + NAMESPACE + '"}[' + DURATION + ']))',
    'RGW gets': 'sum by(instance) (irate(ceph_rgw_get{namespace="' + NAMESPACE + '"}[' + DURATION + ']))',
    'RGW puts': 'sum by(instance) (irate(ceph_rgw_put{namespace="' + NAMESPACE + '"}[' + DURATION + ']))',
    'RGW Failed Req': 'sum by(instance) (irate(ceph_rgw_failed_req{namespace="' + NAMESPACE + '"}[' + DURATION + ']))'
}

# get graphs class
graphs_class = Graphs()
# get dict containing all queried graphs
graphs_dict = graphs_class.get_graphs_from_queries(queries, sum_by="instance")


# logic for displaying graphs
if not GET_GRAPHS_AS_SINGLE_DF:
    # print multiple graphs
    print_dataframe_dict(graphs_dict)
else:
    # print graphs as single dataframe
    graphs_df = graphs_class.get_graphs_as_one_df(
        graphs_dict, sum_by="instance")
    print(graphs_df)

if VISUALIZE_GRAPHS:
    # display graphs in another window
    display_graphs(graphs_dict, sum_by="instance")
