from header import *

# 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", \
# namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics",\
#  cluster="", namespace="' + NAMESPACE + '", resource="cpu"})'

# query= 'rate(container_cpu_usage_seconds_total{namespace="wifire-quicfire"}[12h])'
# timefilter = 'start=2023-07-30T20:10:30.781Z&end=2023-07-31T20:50:00.781Z&step=15s'
# endpoint = f'query_range?query={query}&{timefilter}'


time_series_list = [
    'container_cpu_usage_seconds_total',
    'sum by(node, pod) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="alto"}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="alto", resource="cpu"})',
    'sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="alto"})'
    'node_namespace_pod_container'

]


# query1 = 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + NAMESPACE + '", resource="cpu"})'
# query2 = 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + NAMESPACE + '", resource="cpu"})'
# query3 = 'sum(node_namespace_pod_container:container_cpu_usage_seconds_total)'
# query4 = 'sum(container_cpu_usage_seconds_total)'
# # query5 = time_series_list[0]
# # query5_5 = 'sum by(node, pod) (container_cpu_usage_seconds_total{namespace="' + NAMESPACE + '"}) / sum by(node, pod) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + NAMESPACE + '", resource="cpu"})'
# query6 = 'sum by(pod,node) (node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{namespace="' + NAMESPACE + '"}) / sum by(pod,node) (kube_pod_container_resource_requests{job="kube-state-metrics", namespace="' + NAMESPACE + '", resource="cpu"})'

# print("\n\n\n\n")
# res_q_6 = query_api_site(query6)
# printc(res_q_6)

# sum = 0
# res_list = get_result_list(res_q_6)
# for node_pod_sum in res_list:
# 	sum += float(node_pod_sum['value'][1])

# print("sum is", sum)


json = query_api_site(time_series_list[1])
printc(json)



# print("\n\n\n\n")
# printc(query_api_site(query1))

# print("\n\n\n\n")
# printc(query_api_site(query2))


# print("\n\n\n\n")
# printc(query_api_site(query3))

# print("\n\n\n\n")
# printc(query_api_site(query4))


# print("\n\n\n\n")
# printc(query_api_site(f'sum by(node, pod) ({query5})'))