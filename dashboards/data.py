# autopep8: off
import sys
import os
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
from helpers.querying import query_data
# autopep8: on


'''
==========================================================================================
NOTE:
This is the dashboard for the following Grafana page: 
https: // grafana.nrp-nautilus.io/d/04feRDPWz/storage-capacity?orgId = 1
==========================================================================================
'''

namespace = "rook"
duration = '6h'
sum_by = 'namespace'
queries_dict = {
    'total_data_stored': 'sum(irate(ceph_pool_stored{namespace="' + namespace + '"}[' + duration + ']))',
    'available_capacity': '(ceph_cluster_total_bytes{namespace="' + namespace + '"}-ceph_cluster_total_used_bytes{namespace="' + namespace + '"})/ceph_cluster_total_bytes{namespace="' + namespace + '"}',
    'total_RAW_data': 'ceph_cluster_total_used_raw_bytes{namespace="' + namespace + '"}',
    'data_stored': '(ceph_pool_stored{namespace="' + namespace + '"}) *on (pool_id) group_left(name)(ceph_pool_metadata{namespace="' + namespace + '"})',
    'RAW_data_with_redundancy': '(ceph_pool_stored_raw{namespace="' + namespace + '"}) *on (pool_id) group_left(name)(ceph_pool_metadata{namespace="' + namespace + '"})'
}


def get_datapoint(query):
    result_list = query_data(query)
    if len(result_list) > 0:
        data_values = []
        for item in result_list:
            value = float(item['value'][1])
            data_values.append(round(value, 2))
        if len(data_values) == 1:
            return data_values[0]
        return data_values
    return None


for title, query in queries_dict.items():
    print(f"{title}: \n{get_datapoint(query)}\n\n\n\n")
