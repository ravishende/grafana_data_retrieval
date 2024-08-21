# For graphing all of the graph data that is stored in dataframes
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from graphs import Graphs
from termcolor import colored
from datetime import datetime as dt


def get_graphs_dict(sum_by: list[str] = ["node", "pod"]):
    # make sure sum_by metrics are all lower case
    if sum_by is not None:
        for i in range(len(sum_by)):
            sum_by[i] = sum_by[i].lower()
    # generate graphs if there is no graphs_dict
    graphs_class = Graphs()
    graphs_dict = graphs_class.get_graphs_dict(
        # filters graphs to only include bpd3-worker pods. displays the worker ensemble
        only_include_worker_pods=False,
        # keep as False. Important for calculating the time and displaying it
        display_time_as_datetime=False,
        # for displaying in the terminal how long each query and graph creation takes
        show_runtimes=False
    )
    return graphs_dict


def plot_graph(graph_title):
    plt.get_current_fig_manager().full_screen_toggle()
    # right is default 0.9. right=0.75 moves the graph left so the legend is not partially cut off.
    plt.subplots_adjust(right=0.75)
    plt.title(graph_title + " Over Time")
    plt.xlabel("Time")
    plt.xticks(rotation=25)


def hue_col_from_sum_by(sum_by: list[str]) -> str:
    hue_column = ""
    # handle sum_by to generate hue
    if sum_by is None:
        hue_column = None
    elif "pod" in sum_by:
        hue_column = "Pod"
    else:
        # if sum_by contains one metric, make that the hue column
        if len(sum_by) == 1:
            hue_column = sum_by[0]
        # otherwise don't contain a hue column since we don't know which metric to use
        else:
            hue_column = None
    return hue_column


# display graphs one at a time in a popup window. After closing one, the next one opens
# Note: if graphs do not have a pod column, change sum_by to be the string or list of strings that graphs are summed by (instead of pod)
def display_graphs(graphs_dict: dict = None, sum_by: list[str] | str = ["node", "pod"]):
    # if sum_by is a single string, convert it to a list containing that string
    if isinstance(sum_by, str):
        sum_by = [sum_by]

    if graphs_dict is None:
        graphs_dict = get_graphs_dict(sum_by)

    hue_column = hue_col_from_sum_by(sum_by)

    print(colored("\nDisplaying graphs in popup window. To exit program, go to window and close all graphs.\n", "green"))
    for graph_title, graph_df in graphs_dict.items():
        if graph_df is None:
            print("Graph has no data:", colored(graph_title, "red"))
            continue

        # show time as datetimes instead of seconds since epoch(01-01-1970)
        graph_df["Time"] = graph_df["Time"].apply(dt.fromtimestamp)

        # Set graph styling and labels.
        if hue_column is not None:
            ax = sns.lineplot(data=graph_df, x="Time",
                              y=graph_title, hue=hue_column)
            sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
        else:
            ax = sns.lineplot(data=graph_df, x="Time", y=graph_title)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))

        # display graph
        plot_graph(graph_title)
        plt.show()


if __name__ == "__main__":
    display_graphs()
