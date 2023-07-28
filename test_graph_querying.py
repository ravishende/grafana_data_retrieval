from utils import *
from inputs import *
from graph import *
from pprint import pprint
from rich import print as printc
from datetime import datetime, timedelta
import matplotlib.pyplot as plt


graph_class = Graph()

data = graph_class.get_graphs(show_time_as_timestamp=False)["CPU Usage"]

for pod_json in data:
	if get_worker_id(pod_json["metric"]["pod"]) != None:
		pod = pod_json['metric']['pod']
		df = pd.DataFrame(pod_json['values'], columns = ["time","value"])
		plt.plot(df["time"],df["value"], label = pod_json["metric"]["pod"])
		plt.xticks(df["time"],rotation=45)

plt.xlabel("Time")
plt.ylabel("CPU Usage")
plt.title("CPU Usage over time")
plt.legend(loc=3, prop={'size': 8})
plt.show()
