import shutil
import pandas as pd
from termcolor import colored
from phase_1 import Phase_1
from phase_2 import Phase_2
from phase_3 import Phase_3
from phase_4 import Phase_4
from work_flow_functions import is_phase_finished, set_phase_finished, initialize_files, reset_phases_progress, set_phase_unfinished

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


'''
========================
    Main Program
========================
'''

# if this is the first time running it with new data, set new_run to True. Otherwise, if you are partway through running it, set new_run to False.
new_run = True
if new_run:
    print(colored(
        "\nATTENTION: new_run is set to True. This means that all phases progress will be reset. \
        \nAny runs gathered in phase_1 will be set to old and will not be regathered next time. \
        \nAre you sure you want to continue?", "red"))
    response = input("type 'y' to continue, resetting the progress. Any other response will continue as if new_run were set to False.\n")
    if response == "y":
        initialize_files()
        reset_phases_progress()

# get instances of Phase classes. Each one has a run() method to run its phase
p_1 = Phase_1()
p_2 = Phase_2()
p_3 = Phase_3()
p_4 = Phase_4()
phases = [p_1, p_2, p_3, p_4]


# run phases that have not yet been run
for i, phase in enumerate(phases):
    # get phase number - start at 1, not 0
    phase_number = i+1
    # if phase has previously been finished, move on to next stage
    if is_phase_finished(phase_number):
        print(colored(f"\n\nPhase {phase_number} has already been completed. Moving on.", "green"))
    # for each unfinished phase, run the phase, then set phase_finished of that stage to True
    else:
        print(colored(f"\n\nBeginning Phase {phase_number}...", "magenta"))
        success = phase.run()
        # if phase not successful, break
        if not success:
            print(colored(f"\nProgram stop caused by phase {phase_number}\n", "magenta"))
            break
        # otherwise, set phase as finished, move on
        set_phase_finished(phase_number)
        print(colored(f"\n\nPhase {phase_number} complete!", "green"))
