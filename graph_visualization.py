# For graphing all of the graph data that is stored in dataframes
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from graphs import Graphs
from termcolor import colored
from datetime import datetime as dt

# display graphs one at a time in a popup window. After closing one, the next one opens
# Note: if graphs do not have a Pod column, change sum_by to be the string that graphs are summed by (instead of Pod)
def display_graphs(graphs_dict=None, sum_by=None):
    # generate graphs if there is no graphs_dict
    if graphs_dict is None:
        graphs_class = Graphs()
        graphs_dict = graphs_class.get_graphs_dict(
            only_include_worker_pods=False,  # filters graphs to only include bpd3-worker pods. displays the worker ensemble
            display_time_as_datetime=False,  # keep as False. Important for calculating the time and displaying it
            show_runtimes=False  # for displaying in the terminal how long each query and graph creation takes
            )

    # handle sum_by to generate hue
    if sum_by is None:
        hue_column = "Pod"
    else:
        hue_column = sum_by

    print(colored("\nDisplaying graphs in popup window. To exit program, go to window and close all graphs.\n", "green"))
    for graph_title, graph_df in graphs_dict.items():
        # skip empty graphs
        if graph_df is None:
            print("Graph has no data:", colored(graph_title, "red"))
            continue

        # show time as datetimes instead of seconds since epoch(01-01-1970)
        graph_df["Time"] = graph_df["Time"].apply(dt.fromtimestamp)

        # Set graph styling and labels. Then display graph
        tick_spacing = 1
        ax = sns.lineplot(data=graph_df, x="Time", y=graph_title, hue=hue_column)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
        plt.get_current_fig_manager().full_screen_toggle()
        plt.subplots_adjust(right=0.75)  # right is default 0.9. right=0.75 moves the graph left so the legend is not partially cut off.
        plt.title(graph_title + " Over Time")
        plt.xlabel("Time")
        plt.xticks(rotation=25)
        plt.show()

if __name__ == "__main__":
    display_graphs()
