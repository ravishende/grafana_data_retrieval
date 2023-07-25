from utils import *
from inputs import *
from pprint import pprint
from datetime import datetime, timedelta



query = 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"})'
queried_data = query_api_site_for_graph(query, assemble_time_filter())
pprint(get_result_list(queried_data)[0]['values'])

