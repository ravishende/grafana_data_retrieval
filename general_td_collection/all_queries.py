queries_by_namespace_pod = {
    'GPU Utilization': 'DCGM_FI_DEV_GPU_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + node + '"}',
    'Memory Copy Utilization': 'DCGM_FI_DEV_MEM_COPY_UTIL * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + node + '"}',
    'Power': 'DCGM_FI_DEV_POWER_USAGE * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + node + '"}',
    'Temperature': 'DCGM_FI_DEV_GPU_TEMP * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + node + '"}',
    'Fan Speed': 'ipmi_fan_speed * on (namespace, pod) group_left(node) node_namespace_pod:kube_pod_info:{node="' + node + '"}'
}
# single cell query
current_gpu_usage_query = 'sum(cDCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + \
    '"})/count(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"})'
# graph queries
graph_queries_no_sum = {
    'Total GPU usage': 'avg_over_time(namespace_gpu_utilization{namespace=~"' + namespace + '"}[5m])',
    'Requested GPUs': 'count(DCGM_FI_DEV_GPU_TEMP{namespace=~"' + namespace + '"})'
}
graph_queries_by_pod = {
    "GPUs utilization % by pod": 'sum(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod) / count(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod)'
}
# table queries
table_queries_by_pod = {
    'GPU Utilization': 'sum(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod) / count(DCGM_FI_DEV_GPU_UTIL{namespace=~"' + namespace + '"}) by (pod)'
}
table_queries_by_pod_model = {
    'GPUs Requested': 'count(DCGM_FI_DEV_GPU_TEMP{namespace=~"' + namespace + '"}) by (pod, modelName)'
}
queries = {
    'Queue length': 'sum by(instance) (ceph_rgw_qlen{namespace="' + cluster + '"})',
    'RGW Cache Hit': 'sum by(instance) (irate(ceph_rgw_cache_hit{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW Cache Miss': 'sum by(instance) (irate(ceph_rgw_cache_miss{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW gets': 'sum by(instance) (irate(ceph_rgw_get{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW puts': 'sum by(instance) (irate(ceph_rgw_put{namespace="' + cluster + '"}[' + duration + ']))',
    'RGW Failed Req': 'sum by(instance) (irate(ceph_rgw_failed_req{namespace="' + cluster + '"}[' + duration + ']))'
}
