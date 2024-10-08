# file settings
PHASE_1_FILES = {
    # PHASE_1_FILES['read'] is the only required user input file
    #  - contains the following columns: run_uuid, ensemble_uuid, run_status, ens_status
    # csv files
    "read": "csvs/phase_1_read.csv",
    "write": "csvs/phase_1_write.csv",
    "runs_df": "csvs/runs_df.csv",
    "training_data": "csvs/training_data.csv",
    # txt files
    "files_not_found": "txts/files_not_found.txt",
    "paths": "txts/paths.txt",
    "new_paths": "txts/new_paths.txt",
    "old_paths": "txts/old_paths.txt",
    "path_directories": "txts/path_directories.txt",
    "old_path_directories": "txts/old_path_directories.txt",
}
PHASE_2_FILES = {
    # csv files
    "read": PHASE_1_FILES["write"],
    "write": "csvs/phase_2_write.csv",
}
PHASE_3_FILES = {
    # csv files
    "read": PHASE_2_FILES["write"],
    "write": "csvs/phase_3_write.csv",
    "query_progress": "csvs/query_progress.csv"
}
PHASE_4_FILES = {
    # csv files
    "read": PHASE_3_FILES["write"],
    "write": "csvs/phase_4_write.csv"
}

# for metrics_and_columns_setup.py
NUM_DURATION_COLS_FILE = "txts/num_duration_cols.txt"

# for work_flow.py
MAIN_FILES = {
    "phases_progress": "txts/phases_progress.txt"
}
