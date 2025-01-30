import sys
import os
import warnings
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

def query_cpu_activity(start:datetime, end:datetime, filter_str:str, timestep:str, timeout_seconds: int = 60, sum_by:list[str]= "_") -> pd.DataFrame:
    graphs = Graphs(query_timeout_seconds=timeout_seconds)
    aggregate_period = timestep
    queries = {
        'cpu_usage': 'increase(container_cpu_usage_seconds_total{' + filter_str + '}['+ aggregate_period + ']) > 0'
        # 'cpu_usage': 'container_cpu_usage_seconds_total'
    }
    # find out what to sum the graphs by
    if sum_by == "_":
        sum_by = []
        for metric in ['node', 'pod']:
            if metric in filter_str:
                sum_by.append(metric)
        if len(sum_by) == 0:
            sum_by = None
    if isinstance(sum_by, str):
        sum_by = [sum_by]
    
    graphs_dict = graphs.get_graphs_from_queries(
        queries, sum_by=sum_by, start=start, end=end, time_step=timestep, progress_bars=False)
    df = graphs_dict['cpu_usage']
    if df is not None:
        df.columns = [col.lower() for col in df.columns]
    return df


def find_user_history(start:datetime, end:datetime, filter_str:str, timeout_seconds: int = 60,
                      sum_by = "_", verbose=False) -> list[datetime]:
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
        return {}

    active_periods = df[df['cpu_usage'] != 0]  # TODO should be absolete bc the query has "> 0"
    periods_list = active_periods['time'].to_list()

    return_dict = {}
    pod_separated_dfs = active_periods.groupby('pod')
    for pod, df in pod_separated_dfs:
        assert pod not in pod_separated_dfs, "duplicate pod - should not be possible"
        return_dict[pod] = df['time'].to_list()

    if verbose:
        print("Unique Times:\n", list(set(periods_list)), "\nNumber of Total Times:", len(periods_list))
        # print(periods_list)
        print("unique pods:", active_periods['pod'].nunique())
        msg = "Number of Unique Times per pod (unique times | number of pods with that many times)"
        print(msg, active_periods.groupby('pod').size().value_counts(), sep="\n")
    
    # return periods_list
    return return_dict

def _find_coarse_periods(history:dict[str, float]):
     # get all course active periods for each pod
    coarse_periods_by_pod: dict[str, list[tuple[str, str]]] = {}
    for pod, timestamps in history.items():
        if len(timestamps) == 0:
            continue
        timestamps.sort()
        query_periods: list[tuple[float, float]] = []
        # find all the chunks that are at least 3 days apart, then query each chunk with a 1 day buffer on each side
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

        coarse_periods_by_pod[pod] = query_periods
    return coarse_periods_by_pod


def _designate_run_boundaries(times: list[float | int], min_break_sec: float | int, 
                             aggregate_period_sec: float | int) -> pd.DataFrame:
    """
    Parameters:
        times: list of timestamps (in seconds) as ints or floats
        min_break_sec: the minimum amount of time between 2 data points to be considered a new run
            - make sure min_break_sec is longer than the timestep used when querying, 
            otherwise each data point will be considered as a new run.

    """
    # TODO: If there is ever at least min_break_sec between 2 times, separate the periods of active usage by saving their start and end times, note that the start time is 10 minutes (timestep) before the first end time that had data.
    run_periods = []
    
    start_time = times[0]
    prev_time = times[0]

    for curr_time in times[1:]:
        inactive_period = curr_time - (prev_time - aggregate_period_sec)
        if inactive_period >= min_break_sec:
            run_periods.append((start_time - aggregate_period_sec, prev_time))
            start_time = curr_time

    # make sure the final period is added
    if run_periods[-1][1] != times[-1]:
        run_periods.append((start_time - aggregate_period_sec, times[-1]))
    
    return pd.DataFrame(run_periods, columns=['start', 'end'])


def _find_fine_periods(coarse_periods:dict[str, float], timestep:str='10m', sum_by='_', timeout_seconds: int = 60):
    """
    Parameters
        coarse_periods: a dict of timestamps, where there was at least some cpu usage within the past 24 hours from the timestamps. Indexed by pod (or sum_by).
        timestep: how in depth to query data for.
            - Timestep does not need to be too small, (10 minutes to an hour is fine) because cpu_usage is based on increase not average. Timestep should be lower than minimum_break
        timout_seconds: how long to wait without activity in a query before quitting
    Returns
        a dict of {pod: dataframe of queried periods (with a 'time' column)}
    """

    # query over each period
    pod_df_dict = {}
    for pod, periods in coarse_periods.items():
        filter_str = f'pod="{pod}"'
        df = pd.DataFrame()
        for (start, end) in periods:
            period_df = query_cpu_activity(start=start, end=end, filter_str=filter_str, timestep=timestep, sum_by=sum_by, timeout_seconds=timeout_seconds)

            if period_df is None:
                continue
            df = pd.concat([df, period_df], ignore_index=True)

        pod_df_dict[pod] = df
        print(df['time'].describe())
    
    return pod_df_dict

def _get_runs_df(fine_active_periods:dict[str, pd.DataFrame],
                queried_timestep:str, minimum_break:str='1h') -> dict[str, pd.DataFrame]:
    """
    Parameters:
        fine_active_periods: a dict of pod:df returned from find_fine_periods
        queried_timestep: the timestep used in find_fine_periods for querying
        minimum_break: the minimum time of 0 cpu_usage to separate two periods
            - Essentially, how much time must elapse after one run for us to count it as a second session
    Returns:
        a dict mapping pod to runs_df with the columns ['start', 'end', 'pod']
    """
    min_break_seconds = time_str_to_delta(minimum_break).total_seconds()
    aggregate_period_sec = time_str_to_delta(queried_timestep).total_seconds()

    pod_dfs_dict = {}
    for pod, df in fine_active_periods.items():
        times = df['time'].to_list()
        runs_df = _designate_run_boundaries(times=times, min_break_sec=min_break_seconds, 
                                           aggregate_period_sec=aggregate_period_sec)
        runs_df[pod] = pod
        pod_dfs_dict[pod] = runs_df
    return  pod_dfs_dict


def find_user_runs(history, timestep:str = '10m', min_break:str = '1h', timeout_seconds:int=60):
    """
    Parameters:
        history: how 
        timestep: resolution with which to query over (resolution for the runs)
        min_break: minimum time of inactive cpu for a period to be considered 2 separate runs

    """
    # TODO: use columns in a single df to designate pod/label differences, don't use a dict of dfs
    # TODO: add batches and save query progress to csvs
    # TODO: add options for saving time column as datetimes or numerical timestamps
    coarse_active_periods = _find_coarse_periods(history)
    fine_active_periods = _find_fine_periods(
        coarse_periods=coarse_active_periods, timestep=timestep,timeout_seconds=timeout_seconds)
    user_runs_dict = _get_runs_df(
        fine_active_periods=fine_active_periods, queried_timestep=timestep, minimum_break=min_break)

    return user_runs_dict


# TODO: handle if queries time out - add message
def main():
    period_start = datetime.now() - timedelta(days=100)
    period_end = datetime.now()
    # POD_SUBSTRING = 'fc-worker-1-'
    # POD_SUBSTRING = 'bp3d-worker-k8s-'
    POD_SUBSTRING = 't1coleman'
    pod_regex_str = f'.*{POD_SUBSTRING}.*'
    filter_str = get_filter_str(pod_regex=pod_regex_str)
    
    active_days = find_user_history(start=period_start, end=period_end,
                                filter_str=filter_str, timeout_seconds=60,
                                sum_by=['Pod'])
    
    user_runs = find_user_runs(history=active_days, timestep='10m',
                               min_break='1h', timeout_seconds=60)
    print(user_runs)

    # TODO: once it is just one df instead of dict of dfs being passed around, delete this
    combined_df = pd.DataFrame()
    for runs_df in user_runs.values():
        combined_df = pd.concat([combined_df, runs_df], ignore_index=True)
    combined_df.to_csv(SAVE_FILE)
    
if __name__ == "__main__":
    main()