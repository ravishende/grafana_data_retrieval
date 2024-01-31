import shutil
import pandas as pd
from work_flow_functions import *
from workflow_files import phase1_files, phase2_files, phase3_files, phase4files

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


# global progress variables - DO NOT EDIT
PHASE_1_COMPLETE = False
PHASE_2_COMPLETE = False
PHASE_3_COMPLETE = False


'''
Work Flow

=========================
Phase 1:  Collecting Runs
=========================
1. get successful bp3d runs from bp3d-runs
    - run_selection.py
2. collect runs from successful bp3d runs  # needs new phase?
    - gather.ipynb in collect_runs/
3. add in ensemble uuid
    - add_id_cols.py 


======================
Phase 2: Preprocessing
======================
4. calculate area and runtime
    - filter_training_data.py 
5. drop drop_cols_1
    - filter_training_data.py
6. add duration_t1, duration_t2 columns
    - query_resources.py

=================
Phase 3: Querying
=================
7. query resource metrics (metrics total, t1, t2)
    - query_resources.py

====================================
Phase 4: Sum and Ready Training Data
====================================
8. sum over json to get floats for resource metrics
    - resource_json_summation.py
9. add in percent columns
    - resource_json_summation.py
10. drop rows with zeros in cpu & mem total
    - investigate_zero_usage.py
11. add ratio cols for t1 & t2
    - add_ratio_cols.py
12. drop drop_cols_2
    - finalize_training_data.py
'''

drop_cols_1 = [
    "path",
    # "time_scraped", # if it's there
    "extent_fmt",
    "dz",
    "fire_grid",
    "output",
    "resolution",
    "resolution_units",
    "run_binary",
    "seed",
    "timestep",
    "topo"
]

drop_cols_2 = [
    "start",
    "stop", 
    "ensemble_uuid", 
    # "run_uuid"
]


'''
============================
        Main Program
============================
'''


'''
========================
Phase 1: Collecting Runs
========================
'''

# 1. get successful bp3d runs from bp3d-runs
runs_df = pd.read_csv(phase1_files['read'], index_col=0)
successful_runs_df = get_successful_runs(runs_df, reset_index=True)

# 2. collect runs from successful bp3d runs
    # - gather.ipynb in collect_runs/
collected_runs_df = collect_runs(successful_runs_df)  # write collect_runs() function logic

# 3. add in ensemble uuid
ids_included_df = add_id_cols(successful_runs_df, collected_runs_df)

ids_included_df.to_csv()

'''
======================
Phase 2: PreProcessing
======================
'''

# 4. calculate area and runtime
calculated_df = add_area_and_runtime(ids_included_df)

# 5. drop drop_cols_1
filtered_df = drop_columns(calculated_df, drop_cols_1)

# 6. add duration_t1, duration_t2 columns
num_duration_cols = 2  # number of duration columns to insert and query for (doesn't include "runtime")
preprocessed_df = insert_n_duration_columns(filtered_df, num_duration_cols, single_method=False)

# save preprocessed_df to file
preprocessed_df.to_csv(phase2_files['write'])
PHASE_1_COMPLETE = True


'''
=================
Phase 3: Querying
=================
'''

# 7. query resource metrics (metrics total, t1, t2)
temporary_save_file = "csv_files/query_progress.csv"
rows_batch_size = 20
queried_df = query_metrics(preprocessed_df, rows_batch_size, temporary_save_file)

# save df to a csv file
queried_df.to_csv(phase3_files['write'])
PHASE_2_COMPLETE = True


'''
====================================
Phase 3: Sum and Ready Training Data
====================================
'''

# 8. sum over json to get floats for resource metrics
summed_df = update_queried_cols(queried_df)

# 9. add in percent columns
percents_included_df = add_percent_columns(summed_df)

# 10. drop rows with zeros in cpu & mem total
nonzero_df = drop_zero_cpu_mem(percents_included_df, reset_index=True)

# 11. add ratio cols for duration_t_i columns and drop numerator columns of ratio cols
ratios_added_df = insert_ratio_columns(nonzero_df, drop_numerators=True, reset_index=True)

# 12. drop_cols_2
final_df = drop_columns(ratios_added_df, drop_cols_2)

# save df to a csv file
queried_df.to_csv(phase4files['write'])
PHASE_3_COMPLETE = True

