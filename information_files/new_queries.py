#__________________________________________________________________________
#                                    GRAPHS
#--------------------------------------------------------------------------


#CPU Usage (6)
'sum(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate{cluster="", namespace="' + NAMESPACE + '"})'

#Memory Usage (w/o cache) (10)
'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '", container!="", image!=""})'

#Bandwidth

	#Receive Bandwidth (16)
	'sum(irate(container_network_receive_bytes_total{cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

	#Transmit Bandwidth (17)
	'sum(irate(container_network_transmit_bytes_total{cluster="", namespace="' + NAMESPACE + '"}[' + duration +']))'

#Rate of Received Packets (19)
'sum(irate(container_network_receive_packets_total{cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

#Rate of Transmitted Packets (20)
'sum(irate(container_network_receive_packets_total{cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

#Rate of Packets Dropped 

	#Rate of Received Packets Dropped (22)
	'sum(irate(container_network_receive_packets_dropped_total{cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

	#Rate of Transmitted Packets Dropped (23)
	'sum(irate(container_network_transmit_packets_dropped_total{cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

#IOPS(Reads+Writes) (25)
# 'ceil(sum(rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="", namespace="' + NAMESPACE + '"}[ts]) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="", namespace="' + NAMESPACE + '"}[' + duration + '])))'
'ceil(sum by(pod) (rate(container_fs_reads_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", namespace="' + NAMESPACE + '"}[' + duration + '])))'

#ThroughPut(Read+Write) (26)
'sum by(pod) (rate(container_fs_reads_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_bytes_total{container!="", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'


#Current Storage IO (28)
	
	#IOPS (Reads) (0)
	'sum by(pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

	#IOPS(Writes) (1)
	'sum by(pod) (rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'
		
	#IOPS(Reads + Writes) (2)
	'sum by(pod) (rate(container_fs_reads_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'
		
	#Throughput(Read) (3)
	'sum by(pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'
		
	#Throughput(Write) (4)
	'sum by(pod) (rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'
		
	#Throughput(Read + Write) (5)
	'sum by(pod) (rate(container_fs_reads_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']) + rate(container_fs_writes_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", device=~"(/dev/)?(mmcblk.p.+|nvme.+|rbd.+|sd.+|vd.+|xvd.+|dm-.+|md.+|dasd.+)", container!="", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'


















#__________________________________________________________________________
#                                    Tables
#--------------------------------------------------------------------------
# #(Outdated)
# #Memory Quota (12)

# 	# Memory Usage (0)
# 	'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '", container!="", image!=""})'

# 	# Memory Requests (1)
# 	'sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="' + NAMESPACE + '"})'

# 	# Memory Requests % (2)
# 	get_percent(memory_usage, memory_requests)

# 	# Memory Limits (3)
# 	'sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="' + NAMESPACE + '"})'

# 	# Memory Limits % (4)
# 	get_percent(memory_usage, memory_limits)

# 	# Memory Usage (RSS) (5)
# 	'sum(container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!=""})'

# 	# Memory Usage (Cache) (6)
# 	'sum(container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!=""})'

# 	# Memory Usage (7)
# 	'sum(container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '",container!=""})'


# #(Outdated)
# #Current Network Usage (14)

# 	#Current Receive Bandwidth (0)
# 	'sum(irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

# 	#Current Transmit Bandwidth (1)
# 	'sum(irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

# 	#Rate of Received Packets (2)
# 	'sum(irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

# 	#Rate of Transmitted Packets (3)
# 	'sum(irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

# 	#Rate of Received Packets Dropped (4)
# 	'sum(irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'

# 	#Rate of Transmitted Packets Dropped (5)
# 	'sum(irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="' + NAMESPACE + '"}[' + duration + ']))'