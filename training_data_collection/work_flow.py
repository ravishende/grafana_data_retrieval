import shutil
import pandas as pd
from termcolor import colored
from phase_1 import Phase_1
from phase_2 import Phase_2
from phase_3 import Phase_3
from phase_4 import Phase_4
from training_data_collection.work_flow_functions import prompt_helitack_status
from work_flow_functions import is_phase_finished, set_phase_finished, display_new_run_prompt, prompt_helitack_status

# display settings
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

'''
NOTE: If this is the first time ever running this file:
all txt and csv files with their paths will be setup for you EXCEPT:
you will need to put in the PHASE_1_FILES['read'] file with the appropiate name into csvs/
Be sure to check workflow_files.py for the proper name of this file, but it is most likely
'phase_1_read.csv' (in the csvs folder). 
It should be a DataFrame containing the following columns:
run_uuid, ensemble_uuid, <-- mandatory
queue_time, run_status, ens_status   <-- optional

Finally, make sure that new_run is set to True at the start of the Main Program below
'''

'''
========================
    User Settings
========================
'''

# if this is the first time running it with new data, set new_run to True. Otherwise, if you are partway through running it, set new_run to False.
new_run = True
# if running into unexpected issues in phase 1 with paths and run uuids not matching up, try setting thorough_refresh to True. Otherwise, keep it False to not waste time regathering paths.
thorough_refresh = False
# if we're looking at helitack (nonstandard - dev) runs, set this to True, otherwise by default, set it to False
is_helitack_run = False

'''
========================
    Main Program
========================
'''

# handle what to do if there is a new run
display_new_run_prompt(new_run, thorough_refresh_status=thorough_refresh)
is_helitack_run = prompt_helitack_status(is_helitack_run)

# get instances of Phase classes. Each one has a run() method to run its phase
# collecting runs and their inputs
p_1 = Phase_1(debug_mode=True)
# preprocessing - calc runtime & area, insert duration cols.
p_2 = Phase_2()
# querying performance metrics - totals and at pseudo-random times
p_3 = Phase_3()
# finalizing data - cleaning, dropping, adding ratio columns, etc.
p_4 = Phase_4(helitack_status=is_helitack_run)
phases = [p_1, p_2, p_3, p_4]

# run phases that have not yet been run
for i, phase in enumerate(phases):
    # get phase number - start at 1, not 0
    phase_number = i+1
    # if phase has previously been finished, move on to next stage
    if is_phase_finished(phase_number):
        # If all phases have already been completed give a message on setting new_run to True
        if phase_number == 4:
            print("Entire work flow has already been completed. Perhaps you would like a new run? If so, set new_run to be True and run again.")
            break
        print(colored(
            f"\n\nPhase {phase_number} has already been completed. Moving on.", "green"))
    # for each unfinished phase, run the phase, then set phase_finished of that stage to True
    else:
        # run the phase and see what the resulting success flag is
        print(colored(f"\n\nBeginning Phase {phase_number}...", "magenta"))
        success = phase.run()
        # if phase isn't successful, break
        if not success:
            print(
                colored(f"\nProgram stop caused by phase {phase_number}\n", "magenta"))
            break
        # otherwise, set phase as finished and move on to the next phase
        set_phase_finished(phase_number)
        print(colored(f"\n\nPhase {phase_number} complete!\n", "green"))
