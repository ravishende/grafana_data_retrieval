import os
import pandas as pd
from termcolor import colored


def reset_files():
    if not os.path.exists('csvs'):
        os.mkdir('csvs')
    csvs_to_reset = [
        'csvs/_query_progress.csv',
        'csvs/queried.csv'
    ]
    empty_df = pd.DataFrame()
    for file in csvs_to_reset:
        empty_df.to_csv(file)


# Handle what to do if it is a new run
def prompt_new_run(new_run):
    if new_run:
        # ask user if they meant to start a new run or continue an old one
        new_run_message = "\
            \nATTENTION: new_run is set to True. This means all previous progress will be reset.\
            \nAre you sure you want to continue?"
        print(colored(new_run_message, "red"))
        response = input(
            "Type 'y' to continue resetting the progress. Any other response will continue as if new_run were set to False.\n")
        # if it is a new run, initialize files and reset progress, wiping files
        if response == "y":
            reset_files()
