import os
import pandas as pd
from multiprocessing.sharedctypes import Value
from workflow_files import MAIN_FILES, PHASE_1_FILES, PHASE_2_FILES, PHASE_3_FILES, PHASE_4_FILES, NUM_DURATION_COLS_FILE

# constants - DO NOT EDIT
NUM_PHASES = 4


# Given contents (a list to write to the file),
# Writes contents to a file. Each element is written on a new line.
# If txt_file does not exist, it is created.
def _write_txt_file(txt_file, contents):
    with open(txt_file, "w") as file:  # Open the file in append mode ('a')
        for entry in contents:
            file.write(entry + "\n")  # Write each entry on a new line

# Given a file path (directories/name), if the file exists, do nothing, otherwise, create the file
def _initialize_file(file_path):
    # Extract the directory path from the file name
    directory = os.path.dirname(file_path)
    
    # If the directory does not exist and the file is in a directory, create it
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        # If the file does not exist, create it
        with open(file_path, 'w'):
            return # we only want to create the file, not write to it.

# make sure all necessary files exist
def initialize_files():
    all_files_dicts = [MAIN_FILES, PHASE_1_FILES, PHASE_2_FILES, PHASE_3_FILES, PHASE_4_FILES]
    
    # get paths to all files so that they can be initialized
    all_file_paths = [NUM_DURATION_COLS_FILE]
    for file_dict in all_files_dicts:
        all_file_paths += file_dict.values()
    
    # initialize all files
    for file_path in all_file_paths:
        _initialize_file(file_path)

    
# given a filename of a txt file, line number, and message, update the line at line_number to be "message", ending in a new line
def _update_txt_line(file_name, line_number, message):
     # read file, then update the phase line to be message with a \n
    try:
        lines = []
        # Read the file's current contents
        with open(file_name, 'r') as file:
            lines = file.readlines()
        
        # update the file's contents
        lines[line_number] = f"{message}\n"
        with open(file_name, 'w') as file:
            file.writelines(lines)
    except Exception as e:
        raise ValueError(f"An unexpected error occurred: {e}")
        

# given a file_name and line_number, return the contents of the line (without any trailing whitespace or \n at the end of the line)
def _read_txt_line(file_name, line_number):
     # read file, then check the line's progress
    try:
        # Read the file's current contents
        with open(file_name, 'r') as file:
            lines = file.readlines()
            line_content = lines[line_number].strip()  # Remove leading/trailing whitespace and newline
            return line_content
    except Exception as e:
        raise ValueError(f"An unexpected error occurred: {e}")
    

# check that the input phase_number is a valid phase
def _check_phase_number(phase_number):
    # check input is a valid phase
    if phase_number not in range(1, NUM_PHASES+1):
        raise ValueError(f"phase_number ({phase_number}) must be an integer between 1 and {NUM_PHASES} inclusive\n")


# given a phase number, clear the write and progress files for that phase
def _wipe_phase_files(phase_number, wipe_paths_gathered=False):
    # check proper input
    _check_phase_number(phase_number)
    if wipe_paths_gathered and phase_number != 1:
        raise ValueError("wipe_paths_gathered can only be true if phase_number is 1. \
            Phase_1 is the only phase that uses paths_gathered")
    
    # set up csv files per phase that need to be cleared
    csv_files_to_reset = {
        1: [PHASE_1_FILES['write'], PHASE_1_FILES['runs_df']],
        2: [PHASE_2_FILES['write']],
        3: [PHASE_3_FILES['write'], PHASE_3_FILES['query_progress']],
        4: [PHASE_4_FILES['write']]
        }
    
    # set up txt files per phase that need to be cleared
    txt_files_to_reset = {
        1: [PHASE_1_FILES['files_not_found']],
        2: [NUM_DURATION_COLS_FILE],
        3: [],
        4: []
    }
    # add paths gathered files to files that need to be cleared if wipe_paths_gathered is True
    if wipe_paths_gathered: 
        txt_files_to_reset[1] += [
            PHASE_1_FILES['paths'], PHASE_1_FILES['new_paths'], PHASE_1_FILES['path_directories']]

    # clear csv files for selected phase
    empty_df = pd.DataFrame()
    for file in csv_files_to_reset[phase_number]:
        empty_df.to_csv(file)
    
    # clear txt files for selected phase
    for txt_file in txt_files_to_reset[phase_number]:
        with open(txt_file, 'w'):
            continue


# given a phase number (1 through 4): return True or False depending on whether the stater has been finished or not
def is_phase_finished(phase_number):
    # check that input is valid
    _check_phase_number(phase_number)
    # get the status of the requested phase
    line_number = phase_number-1
    phase_status = _read_txt_line(MAIN_FILES["phases_progress"], line_number)
    # return True if phase_status is 'T', or False otherwise
    return phase_status == 'T'


# given a phase number (1 through 4): set the phase's 'finished' progress to be True
def set_phase_finished(phase_number):
    # check input is a valid phase
    _check_phase_number(phase_number)
    
    # make sure phases aren't being set incorrecly out of order
    if phase_number > 1:
        if not is_phase_finished(phase_number-1):
            raise ValueError(f"Phase {phase_number} cannot be set as finished because phase {phase_number-1} is still unfinished. Phases must be done sequentially.")

    # write the line number to be "T" (True) at the line corresponding to the phase_number in the phases_progress txt file
    line_num = phase_number-1
    _update_txt_line(file_name=MAIN_FILES['phases_progress'], line_number=line_num, message="T")


# given a phase number (1 through 4): set the phase's 'finished' progress to be False
# if wipe_files is set to True, all progress and write files for the phase will be cleared
def set_phase_unfinished(phase_number, wipe_files=False):
    # check input is a valid phase
    _check_phase_number(phase_number)
    # make sure phases aren't being set incorrecly out of order
    if phase_number < NUM_PHASES:
        if is_phase_finished(phase_number+1):
            raise ValueError(f"Phase {phase_number} cannot be set as unfinished because phase {phase_number+1} is finished. Phases must be done sequentially.")
    
    # write the line number to be "F" (False) at the line corresponding to the phase_number in the phases_progress txt file
    line_num = phase_number-1
    _update_txt_line(file_name=MAIN_FILES['phases_progress'], line_number=line_num, message="F")

    # if requested, wipe the progress files and phase files for the phase given
    if wipe_files:
        _wipe_phase_files(phase_number)


# given the total number of phases, set the progress of all phases to False
# also wipe progress & write files if requested, and wipe paths_gathered file if requested (not recommended)
def reset_phases_progress(wipe_files=False, wipe_paths_gathered=False):
    # handle invalid user input
    if wipe_paths_gathered and not wipe_files:
        raise ValueError(
            "wipe_paths_gathered can only be True if wipe_files is also True.\
            Paths gathered files can only be wiped if progress and write files are also wiped.")
    
    # if file does not have the proper number of phases represented, 
    if len(MAIN_FILES['phases_progress']) != NUM_PHASES+1:
        # rewrite file, setting all phases to incomplete
        _write_txt_file(
            txt_file=MAIN_FILES['phases_progress'], 
            contents=['F' for _ in range(NUM_PHASES)])
        # if not wiping files, work is done
        if not wipe_files:
            return

    # otherwise, set all phases to False - loop from highest til 1 to avoid out of order errors
    for phase_number in range(NUM_PHASES, 0, -1):
        set_phase_unfinished(phase_number, wipe_files=False)

    # wipe files if requested
    if wipe_files:
        # wipe phase 1 according to wipe_paths_gathered
        if wipe_paths_gathered:
            _wipe_phase_files(1, wipe_paths_gathered=True)
        else:
            _wipe_phase_files(1)
        
        # wipe phases 2 through 4
        for phase_number in range(2, NUM_PHASES+1):
            _wipe_phase_files(phase_number)