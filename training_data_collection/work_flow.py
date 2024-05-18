import shutil
import pandas as pd
from termcolor import colored
from phase_1 import Phase_1
from phase_2 import Phase_2
from phase_3 import Phase_3
from phase_4 import Phase_4
from work_flow_functions import is_phase_finished, set_phase_finished, initialize_files, reset_phases_progress, prompt_set_num_duration_cols

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

'''
NOTE: If this is the first time ever running this file:
all txt and csv files with their paths will be setup for you EXCEPT:
you will need to put in the PHASE_1_FILES['read'] file with the appropiate name into csvs/
Be sure to check workflow_files.py for the proper name of this file, but it is most likely
'phase_1_read.csv'. 
It should be a DataFrame containing the following columns:
run_uuid, ensemble_uuid, <-- mandatory
run_status, ens_status   <-- optional

Finally, make sure that new_run is set to True at the start of the Main Program below
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
        "\nATTENTION: new_run is set to True. This means that all phases' progress will be reset.\
        \nEverything except paths in phase_1 will have to be regathered.\
        \nAre you sure you want to continue?", "red"))
    response = input("Type 'y' to continue resetting the progress. Any other response will continue as if new_run were set to False.\n")
    if response == "y":
        initialize_files()
        reset_phases_progress(wipe_files=True)
        # initialize num_duration_cols by asking the user how many partial duration columns to use
        prompt_set_num_duration_cols()

# get instances of Phase classes. Each one has a run() method to run its phase
p_1 = Phase_1()  # for collecting runs and their inputs
p_2 = Phase_2()  # for preprocessing - calculating runtime & area, inserting duration cols, etc.
p_3 = Phase_3()  # for querying performance metrics - totals and at pseudo-random times
p_4 = Phase_4()  # for finalizing data - cleaning, dropping, adding ratio columns, etc.
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
        # run the phase and see what the resulting success flag is
        print(colored(f"\n\nBeginning Phase {phase_number}...", "magenta"))
        success = phase.run()
        # if phase isn't successful, break
        if not success:
            print(colored(f"\nProgram stop caused by phase {phase_number}\n", "magenta"))
            break
        # otherwise, set phase as finished and move on to the next phase
        set_phase_finished(phase_number)
        print(colored(f"\n\nPhase {phase_number} complete!\n", "green"))