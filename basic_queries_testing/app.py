from header import *
from tables import *
from graphs import *
from termcolor import cprint, colored

headers = Header()
tables = Tables()
graphs = Graphs()


print("\n\n\n\n********************************************************************************")
print(colored("                Header:", "magenta"))
print("********************************************************************************")
headers.print_header_data()


print("\n\n\n\n********************************************************************************")
print(colored("                Tables:", "magenta"))
print("********************************************************************************")
tables.print_tables()

print("\n\n\n\n********************************************************************************")
print(colored("                Graphs:", "magenta"))
print("********************************************************************************")
graphs.print_graph_data() #this can take a while to gather all the graph data.


