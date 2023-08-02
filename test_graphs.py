from utils import *
from inputs import *
from graph import *
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import time


def print_graphs(graphs, only_print_worker_pods=False):
	for graph in graphs:
		#split data by pod
		print("\n\n\n\n")
		graph_groups = graph.groupby('Pod')

		#print out graph of each pod
		for pod in graph['Pod'].unique():
			worker_id = get_worker_id(pod)
			if only_print_worker_pods:
				if worker_id != None:
					print("\n\n\n")
					printc(graph_groups.get_group(pod))
			else:
				print("\n\n\n")
				printc(graph_groups.get_group(pod))

# def display_graphs(graph_class):
	# graph_list = graph_class.get_graphs(display_time_as_timestamp=True)


	#display graphs

	# 	#collect the data from the graph
	# 	# # for pod_json in graph_df:
	# 	# 	if get_worker_id(graph_df[pod]) != None:
	# 	# 		# pod = pod_json['metric']['pod']
	# 	# 		# df = pd.DataFrame(pod_json['values'], columns = ["time","value"])
	# 	for pod in graph_df['Pod']:

	# 			plt.plot(df["time"],df["value"], label = pod_json["metric"]["pod"])
	# 			plt.xticks(df["time"],rotation=80)

		#put data into a numpy readable format
		# y_np_list = np.array(y_data, dtype=np.float64)
		# plt.xlabel("Time")
		# plt.ylabel(graph_title)
		# plt.title(f'{graph_title} over time')
		# plt.legend(loc=3, prop={'size': 8})
		# plt.show()

		# #for showing the next graph after the user closes the current one
		# time.sleep(0.2)
		# plt.close()

#Either run display_graphs(graph_class) or graph_class.print_graphs() depending on if you want data visualized or just printed
graph_class = Graph()
graphs = graph_class.get_graphs()
print_graphs(graphs, only_print_worker_pods=False)
