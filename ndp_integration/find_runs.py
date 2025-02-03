import sys
import os
import warnings
import re
import pandas as pd
from datetime import datetime, timedelta
# get set up to be able to import helper functions from parent directory (grafana_data_retrieval)
# Adjust the path to go up one level
sys.path.append("../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
# pylint: disable=wrong-import-position
from helpers.time_functions import datetime_ify, time_str_to_delta
from graphs import Graphs

SECONDS_PER_DAY = 86400
PROGRESS_FILE = 'csvs/_query_progress.csv'
SAVE_FILE = 'csvs/write.csv'

def _get_component_filter_str(component: str, component_name: str = None,
                              component_regex: str = None) -> str:
    if component_name and component_regex:
        raise ValueError(
            "at most one of component_name or component_regex should be defined.")
    # give a warning if it doesn't seem like the filter str is being used correctly
    known_k8s_components = ['node', 'pod', 'namespace', 'cluster', 'job', 'instance',
                            'instance_id', 'container', 'Hostname', 'UUID', 'device',
                            'endpoint', 'service', 'prometheus', 'service']
    if component not in known_k8s_components:
        warnings.warn(
            f"Unknown component '{component}'. Known components are: {known_k8s_components}")
    # give the string depending on if it's a regex expression or not
    if component_name:
        return f'{component}="{component_name}"'
    if component_regex:
        return f'{component}=~"{component_regex}"'
    # neither are defined
    return ""

def get_filter_str(node: str | None = None, node_regex: str | None = None,
                   pod: str | None = None, pod_regex: str | None = None,
                   namespace: str | None = None, namespace_regex: str | None = None) -> str:
    """creates the filter settings used in querying based on the passed in filter parameters

    Args:
        node: the node to filter on
        node_regex: regex pattern to include any nodes that match the pattern
        pod: the pod to filter on
        pod_regex: regex pattern to include any pods that match the pattern
        namespace: the namespace to filter on
        namespace_regex: regex pattern to include any namespaces that match the pattern

    Returns:
        the new filter string
    """
    # get individual filters
    node_filter = _get_component_filter_str("node", node, node_regex)
    pod_filter = _get_component_filter_str("pod", pod, pod_regex)
    namespace_filter = _get_component_filter_str("namespace", namespace, namespace_regex)

    # assemble filter str
    filters = [node_filter, pod_filter, namespace_filter]
    filter_str = ""
    for filt in filters:
        if filt == "":
            continue
        # in PromQL, it still works if the filter string ends in a comma
        filter_str += filt + ', '

    if filter_str == "":
        warnings.warn("No filters specified. This will likely cause any queried data to be inaccurate.")
        
    return filter_str

def filter_str_to_sum_by(filter_str:str) -> str | None:
    """ get the sum_by list from a filter_str.
    """
    # find all of the metrics specified in the filter str (by finding matches btwn a ',' and '=')
    # ex: new_filter_str = ',pod="x", node="y"' --> sum_by = ['pod', 'node']
    new_filter_str = ',' + filter_str
    sum_by = re.findall(r",\s*(.*?)=", new_filter_str)
    if len(sum_by) == 0:
        sum_by = None
    return sum_by

def query_cpu_activity(start:datetime, end:datetime, filter_str:str, timestep:str, timeout_seconds: int = 60, sum_by:list[str]= "_") -> pd.DataFrame:
    graphs = Graphs(query_timeout_seconds=timeout_seconds)
    aggregate_period = timestep
    queries = {
        'cpu_usage': 'increase(container_cpu_usage_seconds_total{' + filter_str + '}['+ aggregate_period + ']) > 0'
        # 'cpu_usage': 'container_cpu_usage_seconds_total'
    }
    # find out what to sum the graphs by, using the filters from the filter string
    if sum_by == "_":
        sum_by = filter_str_to_sum_by(filter_str)
    if isinstance(sum_by, str):
        sum_by = [sum_by]

    graphs_dict = graphs.get_graphs_from_queries(
        queries, sum_by=sum_by, start=start, end=end, time_step=timestep, progress_bars=False)
    df = graphs_dict['cpu_usage']
    if df is not None:
        df.columns = [col.lower() for col in df.columns]
    return df


def find_user_history(start:datetime, end:datetime, filter_str:str, timeout_seconds: int = 60,
                      sum_by = "_", verbose=False) -> pd.DataFrame:
    """
    Given a start time, end time, and filter string, find all of the days that the given pod was active. Look at the cpu usage to do so.
    """
    start = datetime_ify(start)
    end = datetime_ify(end)

    duration_seconds = (end - start).total_seconds()

    timestep = "1d"
    if duration_seconds <= SECONDS_PER_DAY:
        timestep = "1h"

    df = query_cpu_activity(start=start, end=end, filter_str=filter_str, 
                            timeout_seconds=timeout_seconds, timestep=timestep, sum_by=sum_by)
    if df is None:
        print("No cpu usage data for the given filter string over the given time period")
        return pd.DataFrame()

    # verify that there is no 0 cpu usage (because the query has > 0)
    assert (df['cpu_usage'] == 0).sum() == 0
    periods_list = df['time'].to_list()

    df = df.sort_values(by=['pod', 'time'])
    df = df.reset_index(drop=True)

    if verbose:
        print("Unique Times:\n", list(set(periods_list)), "\nNumber of Total Times:", len(periods_list))
        # print(periods_list)
        print("unique pods:", df['pod'].nunique())
        msg = "Number of Unique Times per pod (unique times | number of pods with that many times)"
        print(msg, df.groupby('pod').size().value_counts(), sep="\n")
    
    return df

def _find_coarse_periods(history_df:pd.DataFrame) -> pd.DataFrame:
    if len(history_df) == 0:
        return history_df
    # label column is the column that isn't 'time' - ex: 'pod', 'node', etc.
    label_cols = [col for col in history_df.columns if col not in ['time', 'cpu_usage']]
    coarse_periods_df = pd.DataFrame()
    # get all course active periods for each label
    for labels, df in history_df.groupby(label_cols):
        if len(df) == 0:
            continue
        timestamps = df['time'].to_list()
        timestamps.sort()
        # find all the chunks that are at least 3 days apart, then query each chunk with a 1 day buffer on each side
        query_periods: list[tuple[float, float]] = []
        start_w_buffer = timestamps[0] - SECONDS_PER_DAY
        prev_timestamp = start_w_buffer
        for i, timestamp in enumerate(timestamps):
            # if we've reached a chunk boundary (3 day gap), add that chunk with buffers to list
            if (timestamp - prev_timestamp) >= (SECONDS_PER_DAY * 3):
                end_w_buffer = prev_timestamp + SECONDS_PER_DAY
                query_periods.append((start_w_buffer, end_w_buffer))
                # start time for the next query
                start_w_buffer = timestamp - SECONDS_PER_DAY
            # make sure the last chunk gets added as well
            elif i == len(timestamps) - 1:
                end_w_buffer = prev_timestamp + SECONDS_PER_DAY
                query_periods.append((start_w_buffer, end_w_buffer))
            prev_timestamp = timestamp

        periods_df = pd.DataFrame(data=query_periods, columns=['start', 'end'])
        periods_df[label_cols] = labels
        coarse_periods_df = pd.concat([coarse_periods_df, periods_df], ignore_index=True)
    return coarse_periods_df


def _designate_run_boundaries(times: list[float | int], min_break_sec: float | int, 
                             aggregate_period_sec: float | int) -> pd.DataFrame:
    """
    Parameters:
        times: list of timestamps (in seconds) as ints or floats
        min_break_sec: the minimum amount of time between 2 data points to be considered a new run
            - make sure min_break_sec is longer than the timestep used when querying, 
            otherwise each data point will be considered as a new run.
    Returns:
        dataframe of runs
    """
    assert len(times) > 1, "times is empty - no run boundaries to separate"
    run_periods = []
    
    start_time = times[0]
    prev_time = times[0]

    for curr_time in times[1:]:
        inactive_period = curr_time - (prev_time - aggregate_period_sec)
        if inactive_period >= min_break_sec:
            run_periods.append((start_time - aggregate_period_sec, prev_time))
            start_time = curr_time
        prev_time = curr_time

    # make sure the final period is added
    if len(run_periods)==0 or run_periods[-1][1] != times[-1]:
        run_periods.append((start_time - aggregate_period_sec, times[-1]))
    
    return pd.DataFrame(run_periods, columns=['start', 'end'])


def _find_fine_periods(coarse_periods_df:pd.DataFrame, timestep:str='10m',
                       sum_by: list[str] | str = '_', timeout_seconds: int = 60) -> pd.DataFrame:
    """
    Parameters
        coarse_periods_df: a dataframe of labels and timestamps, where there was at least some cpu usage within the past 24 hours from the timestamps.
        timestep: a promql time string of how in depth to query data for.
            - Timestep does not need to be too small, (10 minutes to an hour is fine) because cpu_usage is based on increase not average. Timestep should be lower than minimum_break
        sum_by: TODO
        timout_seconds: how many seconds to wait without activity in a query before quitting
    Returns
        a dataframe of queried periods (with some label column and a 'time' column)}
    """
    label_cols = [col for col in coarse_periods_df.columns if col not in ['start', 'end', 'cpu_usage']]

    # query over each period
    fine_periods_df = pd.DataFrame()
    for labels, periods_df in coarse_periods_df.groupby(label_cols):
        filter_str_components = [f'{label_col}="{label}",' for label_col, label in zip(label_cols, labels)]
        filter_str = ' '.join(filter_str_components)
        df = pd.DataFrame()
        for tup in periods_df.itertuples():
            start, end = tup.start, tup.end
            fine_df = query_cpu_activity(
                start=start, end=end, filter_str=filter_str,
                timestep=timestep, sum_by=sum_by, timeout_seconds=timeout_seconds)

            if fine_df is None:
                continue
            
            df = pd.concat([df, fine_df], ignore_index=True)
        df[label_cols] = labels
        fine_periods_df = pd.concat([fine_periods_df, df], ignore_index=True)
    return fine_periods_df

def _get_runs_df(fine_periods_df:pd.DataFrame,
                queried_timestep:str, minimum_break:str='1h') -> dict[str, pd.DataFrame]:
    """
    Parameters:
        fine_periods_df: df with columns [label, 'time'] returned from find_fine_periods
        queried_timestep: the timestep used in find_fine_periods for querying
        minimum_break: the minimum time of 0 cpu_usage to separate two periods
            - Essentially, how much time must elapse after one run for us to count it as a second session
    Returns:
        a dict mapping pod to runs_df with the columns [label, 'start', 'end']
    """
    min_break_seconds = time_str_to_delta(minimum_break).total_seconds()
    aggregate_period_sec = time_str_to_delta(queried_timestep).total_seconds()

    label_cols = [col for col in fine_periods_df.columns if col not in ['time', 'cpu_usage']]

    final_df = pd.DataFrame()
    for labels, df in fine_periods_df.groupby(label_cols):
        times = df['time'].to_list()
        runs_df = _designate_run_boundaries(times=times, min_break_sec=min_break_seconds,
                                           aggregate_period_sec=aggregate_period_sec)
        runs_df[label_cols] = labels
        final_df = pd.concat([final_df, runs_df], ignore_index=True)
    final_df = final_df.sort_values(by = label_cols + ['start'])
    return final_df


def find_user_runs(history_df, timestep:str = '10m', min_break:str = '1h', 
                   timeout_seconds:int=60, display_time_as_datetime:bool=True) -> pd.DataFrame:
    """
    Parameters:
        history_df: df containing pod and time columns, where time is a 24 hour period where each one had a 
        timestep: resolution with which to query over (resolution for the runs)
        min_break: minimum time of inactive cpu for a period to be considered 2 separate runs

    """
    if time_str_to_delta(timestep).total_seconds() >= time_str_to_delta(min_break).total_seconds():
        warnings.warn("timestep resolution should be less than min_break. Otherwise breaks may be inaccurate")
    
    if len(history_df) == 0:
        print("\nhistory_df is empty - no runs to find")
        return pd.DataFrame()
    
    coarse_active_periods = _find_coarse_periods(history_df)
    fine_active_periods = _find_fine_periods(coarse_periods_df=coarse_active_periods,   
                                            timestep=timestep, timeout_seconds=timeout_seconds)
    user_runs_df = _get_runs_df(
        fine_periods_df=fine_active_periods, queried_timestep=timestep, minimum_break=min_break)
    
    if display_time_as_datetime and len(user_runs_df) > 0:
        user_runs_df['start'] = user_runs_df['start'].apply(datetime_ify)
        user_runs_df['end'] = user_runs_df['end'].apply(datetime_ify)

    return user_runs_df


def find_ndp_user_runs(username:str, start:datetime, end:datetime = None, timestep:str = '10m',
                       min_break:str ='1h', timeout_seconds:int = 60) -> pd.DataFrame:
    """
    Finds all jupyterhub runs for a user within a given time period and puts them in a dataframe
    Parameters:
        username: the username of the ndp user.
        start: the datetime representing when to start looking for the users' past runs. 
            - No runs done before start will be included in the final dataframe.
        end: the datetime representing when to stop looking for the users' past runs.
            - default is datetime.now().
        timestep: promql time string representing resolution of how often to query data points for
            - recommended to be at least 3 times smaller than min_break
            - for more info on time strings: https://prometheus.io/docs/prometheus/latest/querying/basics/#float-literals-and-time-durations
        min_break: promql time string representing the min time of no cpu usage to separate runs
            - in other words, the limit for how long a single run can be inactive.
            - if min_break is large, this could separate the different sessions where a user logs in and starts running code. If min_break is small, it could separate the individual executions of code cells by the user in one session.
        timeout_seconds: how many seconds to wait without activity in a query before quitting
    Returns:
        a dataframe of the users history of runs with start and end times and a pod column. 
            - If there are multiple pods with the username as a substring, all matching pods will be included in the dataframe. 
    """

    if end is None:
        end = datetime.now()
    assert isinstance(start, datetime), "start must be a of type datetime.datetime"
    assert isinstance(end, datetime), "end must be None or of type datetime.datetime"
    assert isinstance(timestep, str), "timestep must be a time string e.g. '1h15m'"
    assert isinstance(min_break, str), "min_break must be a time string e.g. '5m30s'"
    assert isinstance(timeout_seconds, int), "timeout_seconds must be an int"
    # get the regex of the username in jupyterhub
    pod_regex_str = f'jupyter-{username}.*'
    filter_str = get_filter_str(pod_regex=pod_regex_str)
    active_days_df = find_user_history(start=start, end=end, filter_str=filter_str, 
                                       timeout_seconds=timeout_seconds, sum_by=['pod'])
    user_runs_df = find_user_runs(history_df=active_days_df, timestep=timestep, 
                               min_break=min_break, timeout_seconds=timeout_seconds)
    return user_runs_df

# def _():
#     period_start = datetime.now() - timedelta(days=100)
#     period_end = datetime.now()
#     # pod_substring = 'fc-worker-1-'
#     # pod_substring = 'bp3d-worker-k8s-'
#     pod_substring = 't1coleman'
#     pod_regex_str = f'.*{pod_substring}.*'
#     filter_str = get_filter_str(pod_regex=pod_regex_str)
    
#     active_days_df = find_user_history(start=period_start, end=period_end,
#                                 filter_str=filter_str, timeout_seconds=60)
#     user_runs = find_user_runs(history_df=active_days_df, timestep='10m',
#                                min_break='1h', timeout_seconds=60)
#     print("\n\nuser runs:", user_runs, "\n", sep='\n')
#     user_runs.to_csv(SAVE_FILE)

def main():
    # settings
    ndp_username = "t1coleman"
    start = datetime.now() - timedelta(days=100)
    end = datetime.now()
    timestep = '5m'  # if timestep is small, run boundaries will be more accurate but queries will take longer
    min_break = '1h'  # timestep should be smaller than min_break

    # find all the runs for the specified user within the specified timeframe
    user_runs_df = find_ndp_user_runs(
        username=ndp_username, start=start, end=end,
        timestep=timestep, min_break=min_break, timeout_seconds=60)

    user_runs_df.to_csv("csvs/save_file.csv")

if __name__ == "__main__":
    main()
