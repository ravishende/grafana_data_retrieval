#!/usr/bin/python3
# -*- coding: utf-8 -*-
from main_functions import *
import pandas as pd

# set pandas display dataframe options to display all columns
pd.set_option('display.max_columns', None)  # for displaying all columns
pd.set_option('display.expand_frame_repr', True)  # for having df break and wrap across multiple lines
pd.set_option('display.precision', 3)  # for how many decimal places to display on floats
# pd.set_option('display.max_rows', None)  # for displaying all rows

# run all code
result_dict = get_all_data(
    only_include_worker_pods=False,  # filters graphs for only worker pods. Updates pod names to be just their ensemble
    display_time_as_timestamp=True,  # displays time in readable format instead of seconds since epoch (01/01/1970)
    show_graph_runtimes=False,  # for displaying in the terminal how long each query and graph creation takes
    get_graphs_as_one_df=False  # puts all graphs into one dataframe instead of a dictionary with multiple graphs. For inputting into database in future
)

# print data and collect graphs info
print_all_data(result_dict)
graphs = result_dict['graphs']

# get info on pods dropped and recovered. Then potentially requeries them at a higher resolution
losses_and_requeried_graphs = \
    check_graphs_losses(
        graphs=graphs,  # if this is not passed in, it will automatically be regenerated. This may take time
        print_info=True,  # prints information on pods dropped/recovered: pod, values dropped/recovered, previous time, time of drop/recovery
        requery=None,  # requeries pods dropped/recovered at higher resolution if True, doesn't if False. When None, prompts user if it should requery after collecting info
        show_runtimes=False,  # for displaying in the terminal how long each query and graph creation takes
        display_time_as_timestamp=True  # displays time in readable format instead of seconds since epoch (01/01/1970)
    )

get_tables_data(
    only_include_worker_pods=True,
    display_time_as_timestamp=False)