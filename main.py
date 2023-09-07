#!/usr/bin/python3
# -*- coding: utf-8 -*-
from main_functions import *
import pandas as pd

# set pandas display dataframe options to display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
# pd.set_option('display.max_rows', None)

# run all code
result_dict = get_all_data(
    only_include_worker_pods=False,
    display_time_as_timestamp=True, 
    show_graph_runtimes=False,
    get_graphs_as_one_df=True
)

print_all_data(result_dict)
graphs = result_dict['graphs']
# if requery is set to none, it will prompt the user in the terminal
# if they would like to requery after the code is finished
losses_and_requeried_graphs = \
    check_graphs_losses(
        graphs=graphs,
        print_info=True,
        requery=None,
        show_runtimes=False,
        display_time_as_timestamp=True
    )
