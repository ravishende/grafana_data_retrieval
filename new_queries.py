#Memory Quota

	# Memory Usage (0)
	'sum(container_memory_working_set_bytes{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", container!="", image!="", pod="' + pod + '"})'

	# Memory Requests (1)
	'sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_requests{cluster="", namespace="wifire-quicfire", pod="' + pod + '"})'

	# Memory Requests % (2)
	_get_percentage(memory_usage, memory_requests)

	# Memory Limits (3)
	'sum(cluster:namespace:pod_memory:active:kube_pod_container_resource_limits{cluster="", namespace="wifire-quicfire", pod="' + pod + '"})'

	# Memory Limits % (4)
	_get_percentage(memory_usage, memory_limits)

	# Memory Usage (RSS) (5)
	'sum(container_memory_rss{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", pod="' + pod + '"})'

	# Memory Usage (Cache) (6)
	'sum(container_memory_cache{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", pod="' + pod + '"})'

	# Memory Usage (7)
	'sum(container_memory_swap{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire",container!="", pod="' + pod + '"})'











#current Network Usage (14)

	#Current Receive Bandwidth (0)
	'sum(irate(container_network_receive_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'

	#Current Transmit Bandwidth (1)
	'sum(irate(container_network_transmit_bytes_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'

	#Rate of Received Packets (2)
	'sum(irate(container_network_receive_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'

	#Rate of Transmitted Packets (3)
	'sum(irate(container_network_transmit_packets_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'

	#Rate of Received Packets Dropped (4)
	'sum(irate(container_network_receive_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'

	#Rate of Transmitted Packets Dropped (5)
	'sum(irate(container_network_transmit_packets_dropped_total{job="kubelet", metrics_path="/metrics/cadvisor", cluster="", namespace="wifire-quicfire", pod="' + pod + '"}[' + ts + ']))'
