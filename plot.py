import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import time
from graph import *
from requests import *

#retrieve graphs
graph_retriever = Graph()
graph_list = graph_retriever.get_graphs(show_time_as_timestamp=True).items()

#display graphs
for graph_title, graph_data in graph_list:
	#set up variables
	x_data = []
	y_data = []

	#collect the data from the graph
	for datapoint in graph_data:
		x_data.append(datapoint[1])
		y_data.append(datapoint[0])
	
	#put data into a numpy readable format
	y_np_list = np.array(y_data, dtype=np.float64)

	#setup and display graph
	plt.title(f'{graph_title} over time')
	plt.xlabel("Time")
	plt.ylabel(graph_title)
	plt.plot(x_data,y_np_list)
	plt.xticks(x_data, rotation=90)
	plt.scatter(x_data,y_np_list,s=30,alpha=1)
	plt.show()

	#for showing the next graph after the user closes the current one
	time.sleep(0.2)
	plt.close()

