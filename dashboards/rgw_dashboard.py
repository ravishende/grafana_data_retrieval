import sys
import os
sys.path.append("../grafana_data_retrieval")  # Adjust the path to go up one level
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from graphs import Graphs
from graph_visualization import display_graphs
from helpers.printing import print_dataframe_dict

# display settings
get_graphs_as_single_df=False
visualize_graphs=False


# define inputs for querying
duration = '1h'
cluster = "rook"
queries = {
    'Queue length':'sum by(instance) (ceph_rgw_qlen{namespace="' + cluster + '"})',
    'RGW Cache Hit':'sum by(instance) (irate(ceph_rgw_cache_hit{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW Cache Miss':'sum by(instance) (irate(ceph_rgw_cache_miss{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW gets':'sum by(instance) (irate(ceph_rgw_get{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW puts':'sum by(instance) (irate(ceph_rgw_put{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW Failed Req':'sum by(instance) (irate(ceph_rgw_failed_req{namespace="' + cluster + '"}[' + duration + ']))'
}

# get graphs class
graphs_class = Graphs()
# get dict containing all queried graphs
graphs_dict = graphs_class.get_graphs_from_queries(queries, sum_by="instance")


# logic for displaying graphs
if not get_graphs_as_single_df:
    # print multiple graphs
    print_dataframe_dict(graphs_dict)
else:
    # print graphs as single dataframe
    graphs_df = graphs_class.get_graphs_as_one_df(graphs_dict)
    print(graphs_df)

if visualize_graphs:
    # display graphs in another window 
    display_graphs(graphs_dict, sum_by="instance")