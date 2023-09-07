from utils import print_heading, print_title, print_sub_title, print_dataframe_dict
from termcolor import colored
from header import Header
from tables import Tables
from graphs import Graphs
import pandas as pd

# create variables for classes
header_class = Header()
tables_class = Tables()
graphs_class = Graphs()


# returns three dicts: one containing all header data,
# one with all tables, and one with all graph data
def get_all_data(only_include_worker_pods=False, display_time_as_timestamp=True, show_graph_runtimes=False, get_graphs_as_one_df=False):
    print("    Retrieving Header Data")
    header_dict = header_class.get_header_dict(
        only_include_worker_pods=only_include_worker_pods
    )

    print("    Retrieving Tables Data")
    tables_dict = tables_class.get_tables_dict(
        only_include_worker_pods=only_include_worker_pods
    )

    print("    Retrieving Graphs Data")
    graphs_dict = graphs_class.get_graphs_dict(
        only_include_worker_pods=only_include_worker_pods,
        display_time_as_timestamp=display_time_as_timestamp,
        show_runtimes=show_graph_runtimes
    )

    return_dict = {
        'header': header_dict,
        'tables': tables_dict,
        'graphs': graphs_dict
    }

    if get_graphs_as_one_df:
        graphs_df = graphs_class.get_graphs_as_one_df(graphs_dict)
        return_dict['graphs'] = graphs_df

    return return_dict


# prints data for headers, tables, and graphs.
def print_all_data(data_dict=None):
    if data_dict is None:
        data_dict = get_all_data()

    print_heading('Header')
    print_dataframe_dict(data_dict['header'])

    print_heading('Tables')
    print_dataframe_dict(data_dict['tables'])

    print_heading('Graphs')
    # check if graphs is a dictionary
    graphs = data_dict['graphs']
    if isinstance(graphs, dict):
        print_dataframe_dict(graphs)
    else:  # graphs is a single df
        print(graphs)




def check_graphs_losses(graphs, print_info=True, requery=None, show_runtimes=False, display_time_as_timestamp=False):
    # check for if graphs was input as single dataframe instead of graph
    if isinstance(graphs, pd.DataFrame):
        # check if there is data in the dataframe
        if len(graphs.index) == 0:
            return {"Losses":None, "requeried":None}
    # graphs is a dict. check if graphs has data
    elif all(value is None for value in graphs.values()):
        return {"Losses":None, "requeried":None}


    # get losses
    graphs_losses_dict = graphs_class.check_for_losses(graphs, print_info=print_info)
    if all(value is None for value in graphs_losses_dict.values()):
        print(colored("No pods were dropped, so no need for requerying.", "green"))
        return {"Losses":None, "requeried":None}

    # prompt the user so requery can be set to True or False
    if requery is None:
        #Prompt if the user would like to requery the graphs
        user_response = input("\n\nWould you like to requery the graphs for zoomed in views of the pod drops and recoveries?\nThis can help determine if data was truly dropped or if the graph just went to zero.\n(y/n)\n")
        if user_response in ['y', 'yes', 'Y', 'Yes']:
            requery = True
        else:
            requery = False
    
    if requery == False:
        return {"losses":graphs_losses_dict, "requeried":None}
    
    # requery is True
    requeried_graphs_dict = graphs_class.requery_graphs(graphs_losses_dict, show_runtimes=show_runtimes)
    print_heading('Requeried Graphs')
    #loop through requeried_graphs_dict and print all requeried graphs
    for graph_title, loss_dict in requeried_graphs_dict.items():
        # print graph title
        print_title(graph_title)

        for category, graphs_list in loss_dict.items():
            # Print Dropped or Recovered
            print_sub_title(category)
            # Print graphs
            for graph in graphs_list:
                # update graphs with correct time columns
                if display_time_as_timestamp:
                    graph['Time'] = pd.to_datetime(graph['Time'], unit="s")
                print(graph, "\n\n")
        
    return {"losses":graphs_losses_dict, "requeried":requeried_graphs_dict}
