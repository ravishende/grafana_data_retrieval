import shutil
import pandas as pd
from termcolor import colored
from phase_1 import Phase_1
from phase_2 import Phase_2
from phase_3 import Phase_3
from phase_4 import Phase_4
from metrics_and_columns_setup import include_all_totals_metrics
from work_flow_functions import (
    is_phase_finished, set_phase_finished, display_new_run_prompt, prompt_helitack_status)

# display settings
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

# ===========================================================================================
# NOTE: If this is the first time ever running this file:
# all txt and csv files with their paths will be setup for you EXCEPT:
# you will need to put in the PHASE_1_FILES['read'] file with the appropiate name into csvs/
# Be sure to check workflow_files.py for the proper name of this file, but it is most likely
# 'phase_1_read.csv' (in the csvs folder).
# It should be a DataFrame containing the following columns:
# run_uuid, ensemble_uuid, <-- mandatory
# queue_time, run_status, ens_status   <-- optional

# Finally, make sure that new_run is set to True at the start of the Main Program below
# ===========================================================================================

# ========================
#     User Settings
# ========================

# If this is the first time running the program with new data, set new_run to True. Otherwise,
# if you are partway through running it, set new_run to False.
NEW_RUN = True
# If running into unexpected issues in phase 1 with paths and run uuids not matching up, try
# setting thorough_refresh to True. Otherwise, keep it False to not waste time regathering paths.
THOROUGH_REFRESH = False
# If we're collecting helitack (nonstandard dev) runs, set this to True, otherwise default to False
IS_HELITACK_RUN = False
# If the "..._total" columns should be more than just cpu_usage and mem_usage, set this to True
ALL_TOTALS_INCLUDED = True

'''
========================
    Main Program
========================
'''
include_all_totals_metrics(ALL_TOTALS_INCLUDED)
# Handle what to do if there is a new run
display_new_run_prompt(NEW_RUN, thorough_refresh_status=THOROUGH_REFRESH)
IS_HELITACK_RUN = prompt_helitack_status(IS_HELITACK_RUN)

# Get instances of Phase classes. Each one has a run() method to run its phase
# Phase 1 - collecting runs and their inputs
p_1 = Phase_1()
# Phase 2 - preprocessing - calculate runtime & area, insert duration cols.
p_2 = Phase_2()
# Phase 3 - querying performance metrics - totals and at pseudo-random times
p_3 = Phase_3()
# Phase 4 - finalizing data - cleaning, dropping columns, adding ratio columns, etc.
p_4 = Phase_4(helitack_status=IS_HELITACK_RUN)
phases = [p_1, p_2, p_3, p_4]

# Run phases that have not yet been run
for i, phase in enumerate(phases):
    # Phases start at 1, not 0
    phase_number = i+1
    # If phase has previously been finished, move on to next stage
    if is_phase_finished(phase_number):
        print(colored(
            f"\n\nPhase {phase_number} has already been completed. Moving on.", "green"))
        if phase_number == len(phases):
            print("\nEntire work flow has already been completed. Perhaps you would like a new run?"
                  "\nIf so, set NEW_RUN to be True and run again.\n")
            break
    # Run each unfinished phase, setting it to finished once it is complete
    else:
        # Run the phase and see what the resulting success flag is
        print(colored(f"\n\nBeginning Phase {phase_number}...", "magenta"))
        success = phase.run()
        if not success:
            print(
                colored(f"\nProgram stop caused by phase {phase_number}\n", "magenta"))
            break

        set_phase_finished(phase_number)
        print(colored(f"\n\nPhase {phase_number} complete!\n", "green"))
