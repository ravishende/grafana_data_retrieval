import seaborn as sns
import matplotlib.pyplot as plt
from graphs import Graphs
from datetime import datetime as dt

# generate graphs dataframes
graphs_class = Graphs()
graphs_dict = graphs_class.get_graphs_dict(
    only_include_worker_pods=False,  # filters graphs to only include bpd3-worker pods. displays the worker ensemble
    display_time_as_timestamp=False,  # keep as False. Important for calculating the time and displaying it
    show_runtimes=False  # for displaying in the terminal how long each query and graph creation takes
    )

# display graphs one at a time in a popup window. After closing one, the next one opens
for graph_title, graph_df in graphs_dict.items():
    # skip empty graphs
    if graph_df is None:
        continue

    tick_spacing = 1
    # show time in seconds since the initial time
    time_0 = graph_df["Time"][0]
    graph_df["Time"] = graph_df.apply(lambda row: row["Time"]-time_0, axis=1)
    my_datetime = dt.fromtimestamp(time_0)
    s = my_datetime.strftime("%H:%M")

    # graph styling and labels
    ax = sns.lineplot(data=graph_df, x="Time", y=graph_title, hue="Pod")
    plt.title(graph_title)
    plt.xlabel("Seconds Since "+s)
    plt.xticks(rotation=25)
    plt.show()
