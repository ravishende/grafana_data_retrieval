from header import Header
from tables import Tables
from graphs import Graphs
from termcolor import colored
import pandas as pd

# set pandas display dataframe options to display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# create variables for classes
header_class = Header()
tables_class = Tables()
graphs_class = Graphs()


# returns three dicts: one containing all header data,
# one with all tables, and one with all graph data
def get_all_data(only_include_worker_pods=False):
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
        display_time_as_timestamp=True,
        show_runtimes=False
    )

    return_dict = {
        'header': header_dict,
        'tables': tables_dict,
        'graphs': graphs_dict        
    }

    return return_dict


# Helper Function: for a given dictionary in the form {titles:dataframes}
# print the title and dataframe of each item in the dict
def print_dict(dictionary):
    for title, dataframe in dictionary.items():
        print("\n\n" + "-"*100, "\n")
        print("            ", colored(title, "green"))
        print("-" * 100, "\n")
        if len(dataframe.index) > 0:
            print(dataframe)
        else:
            print(colored("No Data", "red"))
        print("\n\n")


# prints data for headers, tables, and graphs.
def print_all_data(data_dict=None):
    if data_dict is None:
        data_dict = get_all_data()

    print("\n\n\n\n" + "*"*100)
    print(colored("                Header:", "magenta"))
    print("*" * 100)
    print_dict(data_dict['header'])

    print("\n\n\n\n" + "*"*100)
    print(colored("                Tables:", "magenta"))
    print("*" * 100)
    print_dict(data_dict['tables'])

    print("\n\n\n\n" + "*"*100)
    print(colored("                Graphs:", "magenta"))
    print("*" * 100)
    print_dict(data_dict['graphs'])


#run all code
result_dict = get_all_data(only_include_worker_pods=False)
print_all_data(result_dict)
# graphs_class._check_for_losses()
