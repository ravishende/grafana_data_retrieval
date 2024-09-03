import os
import math
import json
import s3fs
import pandas as pd
from tqdm import tqdm
from termcolor import colored
from dotenv import load_dotenv
from workflow_files import PHASE_1_FILES

# settings
pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)


# =========================
# Phase 1:  Collecting Runs
# =========================
# 1. get successful bp3d run ids from read file
# 2. collect runs from successful bp3d run ids

# FINISH:
# save df to a file


class Phase_1():
    # files are only read, so pylint: disable=dangerous-default-value
    def __init__(self, files: dict[str, str] = PHASE_1_FILES,
                 verbose: bool = True, debug_mode: bool = False) -> None:
        self.verbose = verbose
        self.debug_mode = debug_mode
        self.files = files
        self.keep_attributes = [
            'canopy_moisture',
            'extent',
            'run_end',
            'run_max_mem_rss_bytes',
            'run_start',
            'sim_time',
            'surface_moisture',
            'threads',
            'wind_direction',
            'wind_speed'
        ]
        # fs and bucket for collecting paths and runs_df
        self.fs = None
        self.bucket = ""
        self._init_fs_and_bucket()

    # runs the whole phase. Returns True if successful, False otherwise
    def run(self, paths_gathered: bool = False) -> bool:
        success = False
        # for if simulation_paths are fully gathered and we're just getting df from runs
        if paths_gathered:
            simulation_paths = self.read_txt_file(self.files['paths'])
            new_paths = self.read_txt_file(self.files['new_paths'])
        # gather simulation paths to be read
        else:
            # gather paths if they are not yet fully gathered
            simulation_paths = self.gather_all_paths(batch_size=5)
            # drop old paths if new_paths are not yet generated
            new_paths = self._drop_old_paths(simulation_paths, method="txt")

        self._print_if_verbose(
            f"simulation paths length:{len(simulation_paths)}")
        self._print_if_verbose(f"New paths length:{len(new_paths)}")

        if len(new_paths) == 0:
            success_msg = "\nNo new runs since last collection! Nothing left to do."
            print(colored(success_msg, "green"))
            # if success is True, we go to the next stage. We shouldn't go to the next stage here
            success = False
            return success

        # get a df that only contains the ids and potentially queue_time of successful runs
        runs_list_df = pd.read_csv(self.files['read'])
        # Make sure there is no unnamed column from if the csv was saved with/without an index col
        unnamed_cols = runs_list_df.columns.str.match('Unnamed')
        runs_list_df = runs_list_df.loc[:, ~unnamed_cols]

        successful_runs_list_df = self._get_successful_runs(
            runs_list_df, reset_index=True)

        # get a df of [path, run_uuid], where we filter new_paths, only including the paths with run_uuids that were successful.
        runs_to_gather_df = self._get_runs_to_gather_df(
            new_paths, successful_runs_list_df)
        final_paths_list = runs_to_gather_df['path'].to_list()

        self._print_if_verbose("getting df from paths\n")
        # get the actual runs from the successful runs paths
        runs_df = self.get_df_from_paths(final_paths_list, batch_size=500)

        self._print_if_verbose("getting finalized dataframe")
        # add ensemble ids to the runs
        result_df = self._merge_dfs(
            runs_data_df=runs_df, successful_runs_list_df=successful_runs_list_df)

        result_df = self._remove_na_rows(result_df, reset_index=True)
        result_df = result_df.rename(
            columns={"run_end": "stop", "run_start": "start"})

        # save final_df
        print(result_df)
        result_df.to_csv(self.files['write'])

        # return phase_1 was successful
        success = True
        return success

    # gather all paths in batches if requested.
    # batch size is a number of paths per batch to get
    def gather_all_paths(self, batch_size: int | None = None) -> list[str]:
        # TODO: when gathering new paths, we append to a txt file. Since we re-gather the last gathered directory, we create duplicates in the txt file even though we don't return them. Make sure duplicates don't get added to the txt file.
        # get all directories and previously gathered directories
        directories = self.fs.ls(self.bucket)
        gathered_directories = self._get_gathered_items("path_directories")
        # the last gathered directory may have new subdirectories. remove it from
        # gathered_directories to account for this
        gathered_directories = gathered_directories[:-1]

        # get list of directories that have not been gathered
        ungathered_directories = [
            d for d in directories if d not in gathered_directories]
        # intialize a list to hold all simulation paths
        simulation_paths_list = self._get_gathered_items("paths")
        # start gathering directories
        progress_msg = f"{len(gathered_directories)} directories have already been gathered. \
            Gathering paths for the remaining {len(ungathered_directories)}. \nThere are {len(directories)} total directories."
        self._print_if_verbose(progress_msg)

        # if we're not using batches, run everything at once
        if batch_size is None:
            new_sim_paths_list = self._get_paths_from_directories(
                ungathered_directories)
            self._append_txt_file(self.files['paths'], new_sim_paths_list)
            # make sure there aren't any duplicates
            all_paths = simulation_paths_list + new_sim_paths_list
            all_unique_paths = list(set(all_paths))
            return all_unique_paths
        # collect runs in batches
        num_batches = math.ceil(len(ungathered_directories)/batch_size)
        for i in range(0, num_batches):
            # get end index for batch. Shouldn't change because
            end_index = batch_size
            # if this is the last iteration, generate paths until the end of ungathered_directories
            if i == num_batches-1:
                end_index = len(ungathered_directories)
            # get simulation paths for this batch
            self._print_if_verbose(
                f"\nGetting paths for {end_index} / {len(ungathered_directories)} directories left.", end=" ")
            self._print_if_verbose(
                f"Batch {i+1}/{num_batches}", "magenta")
            sim_paths_batch = self._get_paths_from_directories(
                ungathered_directories[:end_index])
            # update all simulation paths and remove newly gathered directories from ungathered directories
            simulation_paths_list += sim_paths_batch
            ungathered_directories = ungathered_directories[end_index:]
            # append newly collected paths to the paths.txt file
            self._append_txt_file(self.files['paths'], sim_paths_batch)
        unique_simulation_paths = list(set(simulation_paths_list))
        return unique_simulation_paths

    # given the simulation paths, create a df containing runs
    # for all paths that have corresponding files
    def get_df_from_paths(self, simulation_paths: list[str], batch_size: int = 1000) -> pd.DataFrame:
        # find out how many runs have been looked at already
        try:
            runs_df = pd.read_csv(self.files['runs_df'], index_col=0)
            files_not_found = self.read_txt_file(self.files['files_not_found'])
            num_gathered_runs = len(runs_df) + len(files_not_found)
            if len(runs_df) > 0:
                runs_df_exists = True
                self._print_if_debug(f"RUN DF EXISTS: {len(runs_df)}," "green")
            else:
                runs_df_exists = False
                self._print_if_debug("RUN DF DOESNT EXIST", "magenta")
        # pylint: disable=bare-except
        except:
            num_gathered_runs = 0
            runs_df_exists = False

        # calculate how many batches to run
        num_batches = math.ceil(
            (len(simulation_paths) - num_gathered_runs) / batch_size)
        self._print_if_debug(
            f'\tnum_batches = {num_batches}\n\tbatch_size = {batch_size}\n\tnum simulation_paths = {len(simulation_paths)}\n\tnum_gathered_runs = {num_gathered_runs}', "magenta")
        # loop over unexplored simulation paths, getting df chunks for each batch
        current_batch = 1
        for start_index in range(num_gathered_runs, len(simulation_paths), batch_size):
            self._print_if_verbose(
                f"batch {current_batch}/{num_batches}:", "green")
            current_batch += 1

            # get the stop_index, making sure not to have it larger than len(simulation_paths)
            stop_index = min(start_index + batch_size, len(simulation_paths))
            # get the df from the runs
            partial_runs_df = self._get_df_chunk(
                start_index, stop_index, simulation_paths)

            # save the df to a file
            if len(partial_runs_df) > 0:
                if runs_df_exists:
                    # append to df and don't rewrite the header if the df already exists
                    partial_runs_df.to_csv(
                        self.files['runs_df'], mode='a', header=False, index=False)
                else:
                    partial_runs_df.to_csv(
                        self.files['runs_df'], mode='w', header=True, index=False)
                    runs_df_exists = True

        # get the total runs df and return it
        runs_df = pd.read_csv(self.files['runs_df'])
        # runs_df = runs_df.reset_index(drop=True)
        return runs_df

    # initalize self.fs and self.bucket
    def _init_fs_and_bucket(self) -> None:
        # get login details from .env file
        if not load_dotenv():
            raise EnvironmentError(
                "Failed to load the .env file. This file should contain the ACCESS_KEY and SECRET_KEY for the s3 file system")
        endpoint = 'https://wifire-data.sdsc.edu:9000'
        access_key = os.getenv("ACCESS_KEY")
        secret_key = os.getenv("SECRET_KEY")

        # login and get fs (file system) and bucket
        fs = s3fs.S3FileSystem(
            key=access_key,
            secret=secret_key,
            client_kwargs={
                'endpoint_url': endpoint,
                'verify': False
            },
            skip_instance_cache=False
        )
        bucket = 'burnpro3d/d'

        # initialize fs and bucket
        self.fs = fs
        self.bucket = bucket

    # Given contents (a list to write to the file),
    # Writes contents to a file. Each element is written on a new line.
    # If txt_file does not exist, it is created.
    def _write_txt_file(self, txt_file: str, contents: list[str]) -> None:
        with open(txt_file, "w", encoding="utf-8") as file:  # Open the file in append mode ('a')
            for entry in contents:
                file.write(entry + "\n")  # Write each entry on a new line

    # Given paths_batch (a list of paths to append to the file),
    # appends a batch of entries to txt_file. Each entry is written on a new line.
    # If txt_file does not exist, it is created.
    def _append_txt_file(self, txt_file: str, batch: list[str]) -> None:
        with open(txt_file, "a", encoding="utf-8") as file:  # Open the file in append mode ('a')
            for entry in batch:
                file.write(str(entry) + "\n")  # Write each entry on a new line

    # given the path to a .txt file, return a list where each line of the
    # txt file is an element in the list
    def read_txt_file(self, txt_file: str) -> list[str]:
        contents = []
        with open(txt_file, "r", encoding="utf-8") as file:
            contents = file.read().splitlines()
        return contents

    # If verbose, print the msg. end is the same as in the built in print() function
    # If color (a string in line with termcolor colors) is passed in, print it with that color
    def _print_if_verbose(self, msg: str, color: str | None = None, end: str = "\n") -> None:
        # don't print if not verbose
        if not self.verbose:
            return
        # print according to whether color was passed in
        if color is not None:
            print(colored(msg, color), end=end)
        else:
            print(msg, end=end)

    # If debug, print the msg. end is the same as in the built in print() function
    # If color (a string in line with termcolor colors) is passed in, print it with that color
    def _print_if_debug(self, msg: str, color: str | None = None, end: str = "\n") -> None:
        # don't print if not verbose
        if not self.debug_mode:
            return
        # print according to whether color was passed in
        if color is not None:
            print(colored(msg, color), end=end)
        else:
            print(msg, end=end)

    # given a subdirectory, return all of the run simulation paths
    def _get_sim_paths(self, subdir) -> list[str]:
        sim_paths = []
        paths = self.fs.ls(subdir)
        for path in paths:
            if "run_" in path:
                sim_paths.append(path)
        return sim_paths

    # given an item type ('paths' or 'path_directories')
    # return all of the previously gathered items of that type.
    # note: only works if workflow_files.PHASE_1_FILES is set up so
    # that item_title and "old"+item_title are both keys of files
    def _get_gathered_items(self, item_title: str) -> list[str]:
        # make sure user input is valid
        valid_item_titles = ['path_directories', 'paths']
        if item_title not in valid_item_titles:
            raise ValueError(
                f'item_title must be one of the following: {valid_item_titles}')

        # if no previously gathered items, get previously gathered items from past gatherings
        gathered_items = self.read_txt_file(self.files[item_title])
        if len(gathered_items) == 0:
            # update gathered_items to contain the old gathered items
            old_item_title = 'old_' + item_title
            gathered_items = self.read_txt_file(self.files[old_item_title])
            self._append_txt_file(self.files[item_title], gathered_items)
        return gathered_items

    # given a list of ungathered directories, return all the paths from those directories.
    def _get_paths_from_directories(self, directories: list[str]) -> list[str]:
        paths = []
        for directory in tqdm(directories):
            subdirectories = self.fs.ls(directory)
            for subdir in subdirectories:
                paths += self._get_sim_paths(subdir)
        # write newly gathered directories to a file so they don't ever have to be regenerated
        self._append_txt_file(self.files['path_directories'], directories)
        return paths

    # get the run_uuid (str) from a path (str)
    def _run_id_from_path(self, path):
        run_uuid_w_prefix = path.split('/')[-1]
        run_uuid = run_uuid_w_prefix.split('_')[-1]
        return run_uuid

    # given a df with a 'path' column, add a new column called 'run_uuid'
    # which gets the run_uuid from the path. Return the new df.
    def _add_run_uuid_col(self, df):
        df['run_uuid'] = df['path'].apply(self._run_id_from_path)
        return df

    # Given a paths list and a method of dropping old paths ("txt" or "training_data")
    # Return a new list of paths that only contains new paths (paths not in old paths)
    # Note: method="txt" should be used by default, unless there is no old_paths.txt file
    def _drop_old_paths(self, paths: list[str], method: str = "txt") -> list[str]:
        # use old_paths.txt file to subtract all old paths from current paths file
        if method == "txt":
            # get a list of old paths
            old_paths = self.read_txt_file(self.files['old_paths'])
            # create a new list of paths that only contains paths not in old_paths
            new_paths = [p for p in paths if p not in old_paths]
            # save new paths to a file
            self._write_txt_file(self.files['new_paths'], new_paths)
            return new_paths

        # get run_uuids from paths, then for each run_uuid in training_data, get rid of that path
        if method == "training_data":
            # get list of run_uuids from training_data
            training_data = pd.read_csv(self.files['training_data'])
            existing_uuids = training_data['run_uuid'].to_list()
            # Use the _run_id_from_path function to extract run_uuid from each path
            new_paths = [p for p in paths if self._run_id_from_path(
                p) not in existing_uuids]
            # save new paths to a file
            self._write_txt_file(self.files['new_paths'], new_paths)
            return new_paths

        # handle incorrect method user error
        raise ValueError("method must be either 'txt' or 'training_data'")

    # given a start and stop index and a list of paths,
    # return a df of runs in the section of paths[start:stop]
    def _get_df_chunk(self, start: int, stop: int, paths: list[str]) -> pd.DataFrame:
        # initialize a list of paths that cause filenotfound errors
        bad_paths = []
        # variable to count the amount of runs missing data (columns)
        runs_missing_data = 0

        # don't try to access out of bounds of path
        if stop > len(paths):
            self._print_if_verbose(
                "stop index out of bounds - updating to be len(paths)", "yellow")
            stop = len(paths)
        if start >= len(paths):
            raise ValueError(
                "start cannot be greater than or equal to len(paths)")

        self._print_if_verbose(
            f"Reading from line {start} to {stop-1}", "green")
        # get each path in the chunk and get the run from that path
        rows = []  # list that will collect row dictionaries to be put into the df
        for path in tqdm(paths[start:stop]):
            # try to get the file from the path
            try:
                name = 'quicfire.zarr'
                with self.fs.open(path + '/' + name + '/.zattrs') as file:
                    run_data = json.load(file)
            # if the file isn't there, append path to bad_paths
            except:
                bad_paths.append(path)
                continue

            # add all the important attributes of the run to the row
            row = run_data

            # if an attribute is not in the row, add it as None
            complete_run = True
            for attr in self.keep_attributes:
                if attr not in run_data:
                    row[attr] = None
                    complete_run = False
            # increment runs_missing_data if the run has columns missing
            if not complete_run:
                runs_missing_data += 1

            # add a path column to the row, then append it to rows
            row['path'] = path
            rows.append(row)

        # if there are no successful rows, return an empty dataframe
        if len(rows) == 0:
            self._print_if_verbose("No runs found for this batch", "red")
            return pd.DataFrame()

        # create the df from all of the rows
        df = pd.DataFrame(rows)
        columns_to_keep = ['path'] + self.keep_attributes
        df = df[columns_to_keep]
        df = self._add_run_uuid_col(df)

        # print file not found files
        if len(bad_paths) > 0:
            self._print_if_verbose(
                "FileNotFound Error on the following Files:")
            for file_path in bad_paths:
                self._print_if_verbose(f"\t{file_path}")
        self._print_if_verbose(
            f"\nFinished reading from {start} to {stop-1}\n", "green")
        num_successful_rows = len(rows)
        self._print_if_debug(
            f"{num_successful_rows} collected runs | {len(bad_paths)} files not found.\n{runs_missing_data} runs were missing at least some data\n")

        # append bad paths to files not found
        self._append_txt_file(self.files['files_not_found'], bad_paths)

        # return df
        return df

    # given a dataframe of runs with ens_status and run_status columns,
    # return a new dataframe with only the successful runs
    def _get_successful_runs(self, df: pd.DataFrame, reset_index: bool = True) -> pd.DataFrame:
        if 'ens_status' not in df.columns or 'run_status' not in df.columns:
            warning_message = "\n\nDataFrame does not have 'ens_status' or 'run_status'. Returning df - will not filter by successful runs.\n\n"
            print(colored(warning_message, "red"))
            return df
        # handle if df is empty
        if len(df) == 0:
            raise ValueError(
                "The read file df is empty; cannot get successful runs from it. Please provide a proper dataframe that contains the columns 'run_uuid', 'ensemble_uuid', 'ens_status', and 'runs_status'.")
        # get a df with only the successful runs
        successful_runs = df[(df["ens_status"].str.lower() == "done")
                             & (df["run_status"].str.lower() == "done")]
        num_removed_runs = len(df) - len(successful_runs)
        self._print_if_verbose(
            f"number of removed unsuccessful runs:{num_removed_runs}")
        # if requested, reset the indices to 0 through end of new df after selection
        if reset_index:
            successful_runs = successful_runs.reset_index(drop=True)
        # keep only the useful columns after ens_status and run_status are no longer needed
        successful_runs = successful_runs.drop(
            columns=['ens_status', 'run_status'])
        return successful_runs

    # get a df of path, run_uuid, where we filter new_paths, only including the paths with run_uuids that were successful.
    # def get_runs_to_gather_df(self, new_paths, successful_runs_list_df):
    #     # Convert new_paths list to a DataFrame
    #     new_paths_df = pd.DataFrame(new_paths, columns=['path'])
    #     # Apply _run_id_from_path function to get run_uuid
    #     new_paths_df['run_uuid'] = new_paths_df['path'].apply(self._run_id_from_path)
    #     # Merge with successful_runs_list_df to filter out unsuccessful runs
    #     result_df = new_paths_df[new_paths_df['run_uuid'].isin(successful_runs_list_df['run_uuid'])]
    #     return result_df
    def _get_runs_to_gather_df(self, new_paths: list[str], successful_runs_list_df: pd.DataFrame) -> pd.DataFrame:
        # Convert new_paths list to a DataFrame
        new_paths_df = pd.DataFrame(data={'path': new_paths})
        # Apply _run_id_from_path function to get run_uuid
        new_paths_df['run_uuid'] = new_paths_df['path'].apply(
            self._run_id_from_path)
        self._print_if_debug(
            f"new paths df length: {len(new_paths_df)}", "green")
        self._print_if_debug(
            f"successful_runs_list_df length: {len(successful_runs_list_df)}", "green")
        # Merge with successful_runs_list_df on 'run_uuid' to include 'ensemble_uuid' and filter out unsuccessful runs
        result_df = pd.merge(
            new_paths_df, successful_runs_list_df, on='run_uuid', how='right')
        self._print_if_debug(
            f"\n\nNA ensembles: \n{result_df['path'].isna().value_counts()}")
        self._print_if_debug(
            f"result df length: {len(result_df)}\n\n\n", "green")
        self._print_if_verbose(result_df)
        return result_df

    # given a df that contains all successful runs and a df that
    def _merge_dfs(self, runs_data_df: pd.DataFrame, successful_runs_list_df: pd.DataFrame) -> pd.DataFrame:
        # select the required columns from successful_runs_list_df
        if len(runs_data_df) == 0:
            raise ValueError(
                "\n\nruns_data_df is empty, so it cannot be merged with successful runs df")
        if len(successful_runs_list_df) == 0:
            raise ValueError(
                "\n\nsuccessful_runs_list_df is empty, so it cannot be merged with runs data df.\nFirst, make sure that there are some successful runs in the read df. If there are, try setting `new_run=True`, `thorough_refresh=True` in work_flow.py and rerunning.\n\n")

        # remove duplicate runs (drop duplicate run ids)
        runs_data_df = runs_data_df.drop_duplicates(subset='run_uuid')
        successful_runs_list_df = successful_runs_list_df.drop_duplicates(
            subset='run_uuid')

        # get only the important columns (not statuses of successful_runs_cols)
        cols_to_keep = ['ensemble_uuid', 'run_uuid']
        if 'queue_time' in successful_runs_list_df.columns:
            cols_to_keep.append("queue_time")
        successful_runs = successful_runs_list_df[cols_to_keep]

        # merge the dataframes on 'run_uuid' with a left join to include only the rows from runs_data_df - merged_df['run_uuid'] is the same as runs_data_df['run_uuid]
        merged_df = pd.merge(
            runs_data_df, successful_runs, on='run_uuid', how='left')

        return merged_df

    # given a df returns an updated df with the NaN time rows removed
    def _remove_na_rows(self, df: pd.DataFrame, reset_index: bool = True) -> pd.DataFrame:
        # crucial columns that need to have data. If they do not, that means the run failed somewhere
        time_cols = ['run_start', 'run_end']

        # Create a new DataFrame that includes rows with NA values in 'start', 'stop', or 'runtime'
        # Then store it in a csv file called na_times
        na_mask = df[time_cols].isna()
        na_rows_df = df[na_mask.any(axis=1)]
        if len(na_rows_df) > 0:
            na_rows_df.to_csv('csvs/na_times.csv', mode='a')

        # Drop columns with NA values in any of the time columns
        df = df.dropna(subset=time_cols)

        # reset the indices to 0 to len(df)-1 if requested
        if reset_index:
            df = df.reset_index(drop=True)

        return df
