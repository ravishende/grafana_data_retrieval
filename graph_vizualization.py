import pandas as pd
import seaborn as sns
import time
import matplotlib.pyplot as plt
from graphs import Graphs
from datetime import datetime as dt

graphs_class = Graphs()
graphs_dict = graphs_class.get_graphs_dict(
    only_include_worker_pods=False,
    display_time_as_timestamp=False,  # unimportant; either way the program converts times to datetime object
    show_runtimes=False
    )

for graph_title, graph_df in graphs_dict.items():
# graph_df = pd.read_csv("cpu_usage.csv")

    tick_spacing = 1

    if graph_df is None:
        continue
    time_0 = graph_df["Time"][0]
    graph_df["Time"] = graph_df.apply(lambda row: row["Time"]-time_0, axis=1)
    my_datetime = dt.fromtimestamp(time_0)
    print(my_datetime)
    s = my_datetime.strftime("%H:%M")

    ax = sns.lineplot(data=graph_df, x="Time", y=graph_title,hue="Pod")
    # ax.set(xlim=0)
    # labels = ["13:44", "2023-08-30 13:50:42.138000128", "2023-08-30 13:55:42.138000128"]
    # ax.set(xticklabels=labels)
    plt.title(graph_title)
    plt.xlabel("Seconds Since "+s)
    plt.xticks(rotation = 25)
    plt.show()
