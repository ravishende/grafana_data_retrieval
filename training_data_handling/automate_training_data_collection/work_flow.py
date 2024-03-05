import shutil
import pandas as pd
from workflow_files import MAIN_FILES
from phase_1 import Phase_1
from phase_2 import Phase_2
from phase_3 import Phase_3
from phase_4 import Phase_4

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)


'''
Work Flow

=========================
Phase 1:  Collecting Runs
=========================
1. get successful bp3d runs from bp3d-runs
2. collect runs from successful bp3d runs and paths
3. add in ensemble uuid

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
# given a phase number (1 through 4): return True or False depending on whether the stater has been finished or not
def is_phase_finished(phase):
    # check input is a valid phase
    if phase not in [1,2,3,4]:
        raise ValueError("phase must be set to either: 1, 2, 3, or 4.")
     # read file, then check the line's progress
    try:
        # Read the file's current contents
        with open(MAIN_FILES['phases_progress'], 'r') as file:
            lines = file.readlines()
            line_content = lines[phase-1].strip()  # Remove leading/trailing whitespace and newline
            return line_content == 'T'

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# given a phase number (1 through 4): set the phase's 'finished' progress to be True
def set_phase_finished(phase):
    # check input is a valid phase
    if phase not in [1,2,3,4]:
        raise ValueError("phase must be set to either: 1, 2, 3, or 4.")
    # read file, then update the phase line to be True
    try:
        # Read the file's current contents
        with open(MAIN_FILES['phases_progress'], 'r') as file:
            lines = file.readlines()
            lines[phase - 1] = 'T\n'

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# given a phase number (1 through 4): set the phase's 'finished' progress to be False
def set_phase_unfinished(phase):
    # check input is a valid phase
    if phase not in [1,2,3,4]:
        raise ValueError("phase must be set to either: 1, 2, 3, or 4.")
    # read file, then update the phase line to be False
    try:
        # Read the file's current contents
        with open(MAIN_FILES['phases_progress'], 'r') as file:
            lines = file.readlines()
            lines[phase - 1] = 'F\n'

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# set the progress of all phases to False
def reset_phase_progess():
    set_phase_unfinished(1)
    set_phase_unfinished(2)
    set_phase_unfinished(3)
    set_phase_unfinished(4)
    

'''
========================
    Main Program
========================
'''

# reset progress if this is the first time running it with new data
# reset_progess()

# get instances of classes
p_1 = Phase_1()
p_2 = Phase_2()
p_3 = Phase_3()
p_4 = Phase_4()

# run each phase if it is not finished
# Phase 1: Collecting Runs
if not is_phase_finished(1):
    print("Beginning Phase 1...")
    p_1.run()
    set_phase_finished(1)

# Phase 2: PreProcessing
if not is_phase_finished(2):
    print("Beginning Phase 2...")
    p_2.run()
    set_phase_finished(2)

# Phase 3: Querying
if not is_phase_finished(3):
    print("Beginning Phase 3...")
    p_3.run()
    set_phase_finished(3)

# Phase 4: Sum and Ready Training Data
if not is_phase_finished(4):
    print("Beginning Phase 4...")
    p_4.run()
    set_phase_finished(4)
