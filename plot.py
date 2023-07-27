import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import time
# from utils import *
from graph import *
from requests import *

graph_retriever = Graph()
graph_list = graph_retriever.get_graphs(show_time_as_timestamp=True).items()
for graph_title, graph_data in graph_list:
	#set up variables
	x_data = []
	y_data = []

	#collect the data from the graph
	for datapoint in graph_data:
		x_data.append(datapoint[1])
		y_data.append(datapoint[0])

	# print("x_data is", colored(x_data, "yellow"))
	# print("x_data's type is", colored(type(x_data[0]), "magenta"))
	# print("y_data is", colored(x_data, "yellow"))
	# print("y_data's type is", colored(type(y_data[0]), "magenta"))
	# x_np_list = np.array(x_data, dtype=np.float64)
	y_np_list = np.array(y_data, dtype=np.float64)


	# any python lists or numpy arrays will do, make sure they have same element count though
	# x_data = [datetime(month=2, year=2023, day=4),datetime(month=2, year=2023, day=23), datetime(month=2, year=2023, day=26)]
	# y_data = [1,3,4]

	plt.title(f'{graph_title} over time')
	plt.xlabel("Time")
	plt.ylabel(graph_title)
	plt.plot(x_data,y_np_list)
	# plt.ylim([-1, 3])
	plt.xticks(x_data, rotation=90)
	plt.scatter(x_data,y_np_list,s=30,alpha=1)
	plt.show()

	time.sleep(0.2)
	plt.close()

