from graphs import Graphs
from graph_visualization import display_graphs
from helpers.printing import print_dataframe_dict

# define inputs
duration = '1h'
cluster = "rook"

new_queries = {
    'Queue length':'sum by(instance) (ceph_rgw_qlen{namespace="' + cluster + '"})',
    'RGW Cache Hit':'sum by(instance) (irate(ceph_rgw_cache_hit{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW Cache Miss':'sum by(instance) (irate(ceph_rgw_cache_miss{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW gets':'sum by(instance) (irate(ceph_rgw_get{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW puts':'sum by(instance) (irate(ceph_rgw_put{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW Failed Req':'sum by(instance) (irate(ceph_rgw_failed_req{namespace="' + cluster + '"}[' + duration + ']))',
}

# get graphs class
graphs_class = Graphs()

# get the graphs dict and print it
graphs_dict = graphs_class.get_graphs_from_queries(new_queries, sum_by="instance")
print_dataframe_dict(graphs_dict)

# uncomment the following lines if you prefer to have the graphs as one df
# graphs_df = get_graphs_as_one_df(graphs_dict)
# print(graphs_df)

# uncomment the following line if you want the graphs visualized
# display_graphs(graphs_dict, sum_by="instance")