from utils import *
from inputs import *
from graph import *
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import time


def display_graphs():
	graph_class = Graph()
	graph_list = graph_class.get_graphs(show_time_as_timestamp=True).items()


	#display graphs
	for graph_title, graph_data in graph_list:
		
		#collect the data from the graph
		for pod_json in graph_data:
			if get_worker_id(pod_json["metric"]["pod"]) != None:
				pod = pod_json['metric']['pod']
				df = pd.DataFrame(pod_json['values'], columns = ["time","value"])
				plt.plot(df["time"],df["value"], label = pod_json["metric"]["pod"])
				plt.xticks(df["time"],rotation=80)

		#put data into a numpy readable format
		# y_np_list = np.array(y_data, dtype=np.float64)
		plt.xlabel("Time")
		plt.ylabel(graph_title)
		plt.title(f'{graph_title} over time')
		plt.legend(loc=3, prop={'size': 8})
		plt.show()

		#for showing the next graph after the user closes the current one
		time.sleep(0.2)
		plt.close()


#print_graphs()
display_graphs()