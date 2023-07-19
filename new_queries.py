#current Netwrok Usage (14)

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