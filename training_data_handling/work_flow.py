import pandas as pd

# settings
phase1_read_file = "csv_files/"
phase1_write_file = "csv_files/"
phase2_read_file = "csv_files/"
phase2_write_file = "csv_files/"
phase3_read_file = "csv_files/"
phase3_write_file = "csv_files/"

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


'''
Work Flow

======================
Phase 1: PreProcessing
======================
1. get successful bp3d runs from bp3d-runs
    - run_selection.py
2. collect runs from successful bp3d runs
    - gather.ipynb in collect_runs/
3. add in ensemble uuid
    - add_id_cols.py
4. calculate area and runtime
    - filter_training_data.py
5. drop drop_cols_1
    - filter_training_data.py
6. add duration_t1, duration_t2 columns
    - query_resources.py

=================
Phase 2: Querying
=================
7. query resource metrics (metrics total, t1, t2)
    - query_resources.py

====================================
Phase 3: Sum and Ready Training Data
====================================
8. sum over json to get floats for resource metrics
    - resource_json_summation.py
9. drop rows with zeros in cpu & mem total
    - investigate_zero_usage.py
10. add ratio cols for t1 & t2
    - add_ratio_cols.py
11. drop drop_cols_2
    - finalize_training_data.py
'''

drop_cols_1 = [
    "path",
    "time_scraped", # if it's there
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
    "run_uuid"
    # with the ratio columns added, the following no longer become useful 
    "cpu_t1", 
    "mem_t1",
    "cpu_t2",
    "mem_t2"
]

