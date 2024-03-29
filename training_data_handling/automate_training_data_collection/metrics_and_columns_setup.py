from itertools import chain
from workflow_files import NUM_DURATION_COLS_FILE


'''
========================
    Helper Functions
========================
'''

# read a txt file written by phase_2 that contains _num_duration_cols
def _init_num_duration_cols():
    # read the file to get _num_duration_cols
    try:
        with open(NUM_DURATION_COLS_FILE,"r") as file:
            num_duration_cols = int(file.read().strip())
        # return if successful
        return num_duration_cols

    # handle errors for reading file
    except FileNotFoundError:
        print(f"Error: File {NUM_DURATION_COLS_FILE} not found.")
    except ValueError:
        print(f"Error: Content of {NUM_DURATION_COLS_FILE} is not an integer.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# initialize _col_names_by_time based on self._num_duration_cols and self._non_static_metrics
def _init_col_names_by_time(_num_duration_cols, _non_static_metrics):
    _col_names_by_time = []
    for i in range(1, _num_duration_cols):
        col_names_t_i = [name + "_t" + str(i) for name in _non_static_metrics]
        _col_names_by_time.append(col_names_t_i)
    return _col_names_by_time


'''
=========================
    Private Variables
=========================
'''

# metrics
_all_metrics = [
    "cpu_usage",
    "mem_usage",
    "cpu_request",
    "mem_request",
    "transmitted_packets",
    "received_packets",
    "transmitted_bandwidth",
    "received_bandwidth"
    ]
_request_metrics = ["cpu_request", "mem_request"]
_no_sum_metrics = _request_metrics
_sum_metrics = [metric for metric in _all_metrics if metric not in _no_sum_metrics]
_static_metrics = ["mem_request"]
_non_static_metrics = [metric for metric in _all_metrics if metric not in _static_metrics]

# duration columns
    # Note: assumes duration columns are in the form "duration_t{N}" where {N} is 
    # an int from 1 to _num_duration_cols inclusive
_num_duration_cols = _init_num_duration_cols()
_duration_col_names = ["duration_t" + str(num) for num in range(1,(_num_duration_cols+1))]
_duration_col_total = "runtime"  # from phase_2

# column names
_static_col_names = _static_metrics
_totals_col_names = [name + "_total" for name in _non_static_metrics]
_col_names_by_time = _init_col_names_by_time(_num_duration_cols, _non_static_metrics)
_all_col_names = _static_col_names + _totals_col_names + list(chain.from_iterable(_col_names_by_time))


'''
========================
    Public Variables
========================
'''

METRICS = {
    "all":_all_metrics,
    "sum": _sum_metrics,
    "no_sum": _no_sum_metrics,
    "static":_static_metrics,
    "non_static": _non_static_metrics
    }

DURATION_COLS = {
    "num_cols": _num_duration_cols,
    "col_names":_duration_col_names,
    "total_col":_duration_col_total
    }

COL_NAMES = {
    "static":_static_col_names,
    "totals":_totals_col_names,
    "by_time":_col_names_by_time,
    "all":_all_col_names
    }

ID_COLS = {
    "ensemble": "ensemble_uuid",
    "run": "run_uuid"
    }

