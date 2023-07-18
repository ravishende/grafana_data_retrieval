all_queries = [
# CPU Utilisation (from requests) (1)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="cpu"}),

# CPU Utilisation (from limits) (2)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="cpu"}),

# Memory Utilisation (from requests) (3)
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="memory"}),

# Memory Utilisation (from limits) (4)
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="memory"}),

# CPU Usage (6)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}),
	scalar(kube_resourcequota{cluster="$cluster", namespace="$namespace", type="hard",resource="requests.cpu"}),
	scalar(kube_resourcequota{cluster="$cluster", namespace="$namespace", type="hard",resource="limits.cpu"}),

# CPU Quota (8)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}),
	sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"}),
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"}),
	sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"}),
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"}),

# Memory Quota (12)
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}),
	sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"}),
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"}),
	sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"}),
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"}),
	sum(container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!=""}),
	sum(container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!=""}),
	sum(container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!=""}),

# Current Network usage (14)
 	sum(irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# Recieve Bandwidth (16)
	sum(irate(container_network_receive_bytes_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval])),


# Transmit Bandwidth (17)
	sum(irate(container_network_transmit_bytes_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# Rate of Recieved packets (19)
	sum(irate(container_network_receive_packets_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# Rate of Transmitted packets (20)
	sum(irate(container_network_transmit_packets_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# Rate of Packets dropped (21)
	# no expression given

# Rate of Recieved Packets dropped (22)
	sum(irate(container_network_receive_packets_dropped_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# Rate of Transmitted Packets dropped (23)
	sum(irate(container_network_transmit_packets_dropped_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# IOPS(Reads+Writes) (25)
	ceil(sum(rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))),

# ThroughPut(Read+Write) (26)
	sum(rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

# Current Storage IO (28)
	sum(rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),
	sum(rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval])),

]




#Organizing Notes
'''
CPU Utilisation (from requests) (1)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="cpu"})

CPU Utilisation (from limits) (2)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="cpu"})

Memory Utilisation (from requests) (3)
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(kube_pod_container_resource_requests{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="memory"}) 

Memory Utilisation (from limits) (4)
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(kube_pod_container_resource_limits{job="kube-state-metrics", cluster="$cluster", namespace="$namespace", resource="memory"})

CPU Usage (6)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"})
	scalar(kube_resourcequota{cluster="$cluster", namespace="$namespace", type="hard",resource="requests.cpu"})
	scalar(kube_resourcequota{cluster="$cluster", namespace="$namespace", type="hard",resource="limits.cpu"})

CPU Quota (8)
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"})
	sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"})
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"})
	sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"})
	sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="$cluster", namespace="$namespace"}) / sum(cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"})

Memory Quota (12)
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""})
	sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"})
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="$cluster", namespace="$namespace"})
	sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"})
	sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!="", image!=""}) / sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="$cluster", namespace="$namespace"})
	sum(container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!=""})
	sum(container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!=""})
	sum(container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace",container!=""})

Current Network usage (14)
	sum(irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

Recieve Bandwidth (16)
	sum(irate(container_network_receive_bytes_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval]))


Transmit Bandwidth (17)
	sum(irate(container_network_transmit_bytes_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

Rate of Recieved packets (19)
	sum(irate(container_network_receive_packets_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

Rate of Transmitted packets (20)
	sum(irate(container_network_transmit_packets_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

Rate of Packets dropped (21)
	no expression given

Rate of Recieved Packets dropped (22)
	sum(irate(container_network_receive_packets_dropped_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

Rate of Transmitted Packets dropped (23)
	sum(irate(container_network_transmit_packets_dropped_total{cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

IOPS(Reads+Writes) (25)
	ceil(sum(rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval])))

ThroughPut(Read+Write) (26)
	sum(rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

Current Storage IO (28)
	sum(rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))
	sum(rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]) + rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="$cluster", namespace="$namespace"}[$__rate_interval]))

'''