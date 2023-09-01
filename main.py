#!/usr/bin/python3
# -*- coding: utf-8 -*-
from main_functions import *
import pandas as pd

# set pandas display dataframe options to display all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
# pd.set_option('display.max_rows', None)

# run all code
result_dict = get_all_data(only_include_worker_pods=False)
print_all_data(result_dict)
graphs_dict = result_dict['graphs']
losses_and_requeried_graphs = check_graphs_losses(graphs_dict, print_info=True, requery=False)



