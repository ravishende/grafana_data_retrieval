from utils import *
from inputs import *
from graph import *
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import time

# make sure to display all columns in an unbroken graph when printing graphs
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', None)

def print_graphs(graphs, group_by_pod=False):
	for graph in graphs:


			# #edit Pod column to be the worker_id for each datapoint or None if not a bp3d worker
			# for i in range(len(graph)):
			# 	graph.at[i, 'Pod'] = get_worker_id(graph.at[i, 'Pod'])

			# #filter out all the None values in the Pod column, leaving only the worker pods in the graph. 
			# graph = graph.query('Pod == None')

		
		if group_by_pod:
			f = Figlet(font='slant')
			print("\n\n\n\n")
			print(f.renderText(graph.columns[2]))
			graph_groups = graph.groupby('Pod')
			#print out graph of each pod
			for pod in graph['Pod'].unique():
				pod_graph = graph_groups.get_group(pod)

				print("\n\n")
				printc(pod_graph)

		# code for group_by_pod already prints out a graph of each pod.
		else:
			print("\n\n\n\n")
			printc(graph)

def display_graphs(graphs):
	#loop through graphs, displaying them one at a time
	for graph in graphs:
		graph_groups = graph.groupby('Pod')

		#split graph by pods to create a new df per pod
		for pod in graph['Pod'].unique():
			pod_graph = graph_groups.get_group(pod)

			#plot the x and y data of a pod.
			plt.plot(pod_graph["Time"], pod_graph.loc[2], label = pod)
			plt.xticks(pod_graph["Time"],rotation=80)

		#for showing the next graph after the user closes the current one
		time.sleep(0.2)
		plt.close()

	#display graphs

	# 	#collect the data from the graph
	# 	# # for pod_json in graph_df:
	# 	# 	if get_worker_id(graph_df[pod]) != None:
	# 	# 		# pod = pod_json['metric']['pod']
	# 	# 		# df = pd.DataFrame(pod_json['values'], columns = ["time","value"])
	# 	for pod in graph_df['Pod']:

				# plt.plot(df["time"],df["value"], label = pod_json["metric"]["pod"])
				# plt.xticks(df["time"],rotation=80)

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
graphs = graph_class.get_graphs(only_worker_pods=False)

print_graphs(graphs, group_by_pod=False)
# display_graphs(graph_class, only_worker_pods=False)

