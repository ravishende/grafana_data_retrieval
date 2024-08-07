# Contains the definitions for all the functions that are called in main.py
from helpers.printing import print_heading, print_title, print_sub_title, print_dataframe_dict
from termcolor import colored
from header import Header
from tables import Tables
from graphs import Graphs
import pandas as pd

# create variables for classes
header_class = Header()
tables_class = Tables()
graphs_class = Graphs()


def get_header_data():
    return header_class.get_header_dict()


def get_tables_data(only_include_worker_pods=True):
    return tables_class.get_tables_dict(
        only_include_worker_pods=only_include_worker_pods,
    )


def get_graphs_data(get_graphs_as_one_df=False, only_include_worker_pods=False, display_time_as_datetime=True, show_runtimes=False):
    graphs_dict = graphs_class.get_graphs_dict(only_include_worker_pods=only_include_worker_pods,
                                               display_time_as_datetime=display_time_as_datetime, show_runtimes=show_runtimes)
    if get_graphs_as_one_df:
        return graphs_class.get_graphs_as_one_df(graphs_dict=graphs_dict)
    return graphs_dict


# returns three dicts: one containing all header data,
# one with all tables, and one with all graph data
def get_all_data(only_include_worker_pods=False, display_time_as_datetime=True, show_graph_runtimes=False, get_graphs_as_one_df=False, get_tables_as_one_df=False):
    # get header data
    print("    Retrieving Header Data")
    header_dict = header_class.get_header_dict(
        only_include_worker_pods=only_include_worker_pods
    )

    # get tables data
    print("    Retrieving Tables Data")
    tables_dict = tables_class.get_tables_dict(
        only_include_worker_pods=only_include_worker_pods
    )

    # get graphs data
    print("    Retrieving Graphs Data")
    graphs_dict = graphs_class.get_graphs_dict(
        only_include_worker_pods=only_include_worker_pods,
        display_time_as_datetime=display_time_as_datetime,
        show_runtimes=show_graph_runtimes
    )

    # set up dict to be returned
    return_dict = {
        'header': header_dict,
        'tables': tables_dict,
        'graphs': graphs_dict
    }

    # change graphs_dict and tables_dict to one df instead of a dict of dataframes if specified
    if get_graphs_as_one_df:
        graphs_df = graphs_class.get_graphs_as_one_df(graphs_dict)
        return_dict['graphs'] = graphs_df

    if get_tables_as_one_df:
        print("getting tables as one df", "\n"*10)
        tables_df = tables_class.get_tables_as_one_df(tables_dict)
        return_dict['tables'] = tables_df
    return return_dict


# prints data for headers, tables, and graphs.
def print_all_data(data_dict=None):
    # if there is no data passed in, generate it
    if data_dict is None:
        data_dict = get_all_data()

    print_heading('Header')
    print_dataframe_dict(data_dict['header'])

    print_heading('Tables')
    # check if tables is a dictionary or single dataframe
    tables = data_dict['tables']
    if isinstance(tables, dict):
        print_dataframe_dict(tables)
    else:  # graphs is a single df
        print(tables)

    print_heading('Graphs')
    # check if graphs is a dictionary or single dataframe
    graphs = data_dict['graphs']
    if isinstance(graphs, dict):
        print_dataframe_dict(graphs)
    else:  # graphs is a single df
        print(graphs)


# get information on dropped/recovered pods and requery if requested. Then return a dict of 'losses' (dropped/recovered pods) and 'requeried' graphs
def check_graphs_losses(graphs, print_info=True, requery=None, show_runtimes=False, display_time_as_datetime=False):
    # check for if graphs was input as single dataframe instead of graph
    if isinstance(graphs, pd.DataFrame):
        # check if there is data in the dataframe
        if len(graphs.index) == 0:
            return {"Losses": None, "requeried": None}
    # graphs is a dict. check if graphs has data
    elif all(value is None for value in graphs.values()):
        return {"Losses": None, "requeried": None}

    # get losses
    graphs_losses_dict = graphs_class.check_for_losses(
        graphs, print_info=print_info)
    if all(value is None for value in graphs_losses_dict.values()):
        print(colored("No pods were dropped, so no need for requerying.", "green"), "\n\n")
        return {"Losses": None, "requeried": None}

    # prompt the user on whether to requery or not
    if requery is None:
        # prompt if the user would like to requery the graphs
        num_potential_drops = 0
        for graph in graphs_losses_dict.values():
            if graph is None:
                continue
            # divide by two because each potential drop has two datapoints: with data, without data
            num_potential_drops += len(graph) // 2
        print(f"\n\nThere were {num_potential_drops} potential drops.\n")
        user_response = input(
            "Would you like to requery the graphs for zoomed in views of the pod drops and recoveries?\nThis can help determine if data was truly dropped or if the graph just went to zero.\n(y/n)\n")
        if user_response in ['y', 'yes', 'Y', 'Yes']:
            requery = True
        else:
            requery = False

    if requery is False:
        return {"losses": graphs_losses_dict, "requeried": None}

    # requery graphs
    requeried_graphs_dict = graphs_class.requery_graphs(
        graphs_losses_dict, show_runtimes=show_runtimes)
    print_heading('Requeried Graphs')
    # loop through requeried_graphs_dict and print all requeried graphs
    for graph_title, loss_dict in requeried_graphs_dict.items():
        print_title(graph_title)

        # loop through dropped and recovered graphs
        for category, graphs_list in loss_dict.items():
            # skip graph if there is no data
            if len(graphs_list) == 0:
                continue
            # Print 'Dropped' or 'Recovered'
            print_sub_title(category)
            # Print graphs
            for graph in graphs_list:
                # update graphs with correct time columns
                if display_time_as_datetime:
                    graph['Time'] = pd.to_datetime(graph['Time'], unit="s")
                print(graph, "\n\n")

    return {"losses": graphs_losses_dict, "requeried": requeried_graphs_dict}
