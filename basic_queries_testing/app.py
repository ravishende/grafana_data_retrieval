from header import *
from tables import *
from graphs import *
from termcolor import cprint, colored


# print(f'\n\n\n\n{colored("Header:", "magenta")}')
headers = Header()
tables = Tables()
graphs = Graphs()
# headers.print_header_data()
# tables.check_success()
print("\n\n\n", tables.print_tables(), "\n\n\n")
# graphs.print_graph_data()


