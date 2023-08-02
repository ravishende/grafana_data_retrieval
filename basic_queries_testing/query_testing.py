# 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", \
# namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics",\
#  cluster="", namespace="' + NAMESPACE + '", resource="cpu"})'

# query= 'rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[12h])'
# timefilter = 'start=2023-07-30T20:10:30.781Z&end=2023-07-31T20:50:00.781Z&step=15s'
# endpoint = f'query_range?query={query}&{timefilter}'


time_series_list = [
    



]