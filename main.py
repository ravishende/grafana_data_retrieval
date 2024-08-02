#!/usr/bin/python3
# -*- coding: utf-8 -*-
from main_functions import *
import pandas as pd
import shutil

# display settings
pd.set_option('display.max_columns', None)  # for displaying all columns
# for having df break and wrap across multiple lines
pd.set_option('display.expand_frame_repr', True)
# for how many decimal places to display on floats
pd.set_option('display.precision', 3)
# for using whole window width
pd.set_option('display.width', shutil.get_terminal_size().columns)
# pd.set_option('display.max_rows', None)  # for displaying all rows

# run all code
result_dict = get_all_data(
    # filters graphs for only worker pods. Updates pod names to be just their ensemble id
    only_include_worker_pods=False,
    # displays graph times in readable format instead of seconds since epoch (01/01/1970)
    display_time_as_datetime=True,
    # displays in the terminal how long each query and graph creation takes
    show_graph_runtimes=False,
    # puts all graphs into one dataframe instead of a dictionary with multiple graphs.
    get_graphs_as_one_df=False,
    # puts all tables into one dataframe instead of a dictionary with multiple tables.
    get_tables_as_one_df=False,
)

# print data and collect graphs info
print_all_data(result_dict)
graphs = result_dict['graphs']

# get info on pods dropped and recovered. Then potentially requeries them at a higher resolution
losses_and_requeried_graphs = \
    check_graphs_losses(
        graphs=graphs,  # if this is not passed in, it will be regenerated. This may take time
        print_info=False,  # prints information on pods dropped/recovered
        # requeries pods dropped/recovered at higher resolution if True, doesn't if False.
        # When None, prompts user if it should requery after collecting info
        requery=None,
        # displays in the terminal how long each query and graph creation takes
        show_runtimes=False,
        # displays time in readable format instead of seconds since epoch (01/01/1970)
        display_time_as_datetime=True
    )
