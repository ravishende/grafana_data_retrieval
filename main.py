from header import *
from tables import *
from graphs import *
from termcolor import colored
from IPython.display import display

#set pandas display dataframe options
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

#create variables for classes
header_class = Header()
tables_class = Tables()
graphs_class = Graphs()


# returns three dicts: one containing all header data, one with all tables, and one with all graph data
def get_all_data():
	print("    Retrieving Header Data")
	header_dict = header_class.get_header_dict()
	
	print("    Retrieving Tables Data")
	tables_dict = tables_class.get_tables_dict()
	
	print("    Retrieving Graphs Data")
	graphs_dict = graphs_class.get_graphs_dict(
		display_time_as_timestamp=True, 
		only_include_worker_pods=False, 
		show_runtimes=False
	)

	return header_dict, tables_dict, graphs_dict

#Helper Function: for a given dictionary in the form {titles:dataframes}, print the title and dataframe of each item in the dict
def print_dict(dictionary):
	for title, dataframe in dictionary.items():
		print("\n\n______________________________________________________________________________\n")
		print("            ", colored(title, "green"))
		print("______________________________________________________________________________\n")
		if len(dataframe.index) > 0:
			print(dataframe)
		else:
			print(colored("No Data", "red")) 
		print("\n\n")

#prints data for headers, tables, and graphs.
def print_all_data(header_dict=None, tables_dict=None, graphs_dict=None):
	if header_dict==None or tables_dict==None or graphs_dict==None:
		header_dict, tables_dict, graphs_dict = get_all_data()

	print("\n\n\n\n********************************************************************************")
	print(colored("                Header:", "magenta"))
	print("********************************************************************************")
	print_dict(header_dict)

	print("\n\n\n\n********************************************************************************")
	print(colored("                Tables:", "magenta"))
	print("********************************************************************************")
	print_dict(tables_dict)

	print("\n\n\n\n********************************************************************************")
	print(colored("                Graphs:", "magenta"))
	print("********************************************************************************")
	print_dict(graphs_dict)


header, tables, graphs = get_all_data()
print_all_data(header, tables, graphs)