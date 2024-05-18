from workflow_files import NUM_DURATION_COLS_FILE
from termcolor import colored


'''
========================
    Helper Functions
========================
'''
# Set the number of partial duration columns (e.g. 2 goes up to duration_t2)
# Note: do not run this between phase 3 and 4, 
# since they require the same amount of duation columns to be defined
def set_num_duration_cols(num, suppress_warning=False):
    # make sure input is an int or convertible to an int
    if not type(num) == int:
        try:
            # make sure that num can be converted to an int and do so for comparisons later
            num = int(num)
        except ValueError:
            raise TypeError("Input must be an int or convertible to an int.")
    
    # if num is greater than a threshold number, give a warning and promt to continue
    max_num_before_warning = 3
    if not suppress_warning and num > max_num_before_warning: 
        # message strings
        warning_message = "\
            \nEach additional duration column is essentially replicating training data runs.\
            \nHaving too many may lead to over fitting.\
            \nAdditionally, it will take a long time to query for."
        check_to_continue = f"\
            \nAre you sure you want to continue? Type 'y' to continue with {num} duration columns.\
            \nAny other response will repromt how many duration columns you would like."
        # print warning and check if the user wants to continue
        print(colored(warning_message, "red"))
        response = input(check_to_continue)
        # if they don't want to continue, rerun the function with a new number they give
        if response != "y":
            updated_num = input("How many partial duration columns would you like?")
            set_num_duration_cols(updated_num)

    # initialize num_duration_cols by writing to its specified file
    num = str(num) # convert num back to a string so that it can be written to the file
    with open(NUM_DURATION_COLS_FILE, "w") as file:
        file.write(num)

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
    
    # return 0 if there is an error (no file setup) to avoid errors before resetting files
    return 0

# initialize _col_names_by_time based on self._num_duration_cols and self._non_static_metrics
def _init_col_names_by_time(_num_duration_cols, _non_static_metrics):
    col_names_by_time = []
    for i in range(1, _num_duration_cols+1):
        col_names_t_i = [name + "_t" + str(i) for name in _non_static_metrics]
        col_names_by_time += col_names_t_i
    return col_names_by_time


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
_static_metrics = ["cpu_request", "mem_request"]
_non_static_metrics = [metric for metric in _all_metrics if metric not in _static_metrics]

# duration columns
    # Note: assumes duration columns are in the form "duration_t{N}" where {N} is 
    # an int from 1 to _num_duration_cols inclusive
_num_duration_cols = _init_num_duration_cols()
_duration_col_names = ["duration_t" + str(num) for num in range(1,(_num_duration_cols+1))]
_duration_col_total = "runtime"  # from phase_2

# column names
_static_col_names = _static_metrics
# If total columns should include nonstatic metrics like received_packets, switch the _totals_col_names definition
_totals_col_names = [name + "_total" for name in _non_static_metrics]
# _totals_col_names = [name + "_total" for name in ["cpu_usage", "mem_usage"]]
_col_names_by_time = _init_col_names_by_time(_num_duration_cols, _non_static_metrics)
_all_col_names = _static_col_names + _totals_col_names + _col_names_by_time


'''
========================
    Public Variables
========================
'''

METRICS = {
    "all":_all_metrics,
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

'''
from workflow_files import NUM_DURATION_COLS_FILE



# ========================
#     Helper Functions
# ========================


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
    
    # return 0 if there is an error (no file setup) to avoid errors before resetting files
    return 0

# initialize _col_names_by_time based on self._num_duration_cols and self._non_static_metrics
def _init_col_names_by_time(_num_duration_cols, _non_static_metrics):
    _col_names_by_time = []
    for i in range(1, _num_duration_cols+1):
        col_names_t_i = [name + "_t" + str(i) for name in _non_static_metrics]
        _col_names_by_time += col_names_t_i
    return _col_names_by_time

# for defining metrics to be exported
def _get_metrics_dict():
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
    _static_metrics = ["cpu_request", "mem_request"]
    _non_static_metrics = [metric for metric in _all_metrics if metric not in _static_metrics]
    # assemble metrics dict
    metrics_dict = {
        "all":_all_metrics,
        "static":_static_metrics,
        "non_static": _non_static_metrics
        }
    return metrics_dict

# duration columns
    # Note: assumes duration columns are in the form "duration_t{N}" where {N} is 
    # an int from 1 to _num_duration_cols inclusive
def _get_duration_cols_dict():
    _num_duration_cols = _init_num_duration_cols()
    _duration_col_names = ["duration_t" + str(num) for num in range(1,(_num_duration_cols+1))]
    _duration_col_total = "runtime"  # from phase_2
    # assemble duration_cols_dict
    duration_cols_dict = {
        "num_cols": _num_duration_cols,
        "col_names":_duration_col_names,
        "total_col":_duration_col_total
        }
    return duration_cols_dict

# column names
def _get_col_names_dict():
    _static_col_names = _static_metrics
    _totals_col_names = [name + "_total" for name in _non_static_metrics]
    _col_names_by_time = _init_col_names_by_time(_num_duration_cols, _non_static_metrics)
    _all_col_names = _static_col_names + _totals_col_names + _col_names_by_time
    # assemble col_names_dict
    col_names_dict = {
        "static":_static_col_names,
        "totals":_totals_col_names,
        "by_time":_col_names_by_time,
        "all":_all_col_names
        }
    return col_names_dict

def _get_id_cols_dict():
    id_cols_dict = {
        "ensemble": "ensemble_uuid",
        "run": "run_uuid"
        }
    return id_cols_dict




# ========================
#     Export Variables
# ========================


METRICS = _get_metrics_dict()
DURATION_COLS = _get_duration_cols_dict()
COL_NAMES = _get_col_names_dict()
ID_COLS = _get_id_cols_dict()


'''