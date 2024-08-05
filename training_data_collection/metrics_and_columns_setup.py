from multiprocessing.sharedctypes import Value
from workflow_files import NUM_DURATION_COLS_FILE
from termcolor import colored


'''
========================
    Helper Functions
========================
'''


# read a txt file written by phase_2 that contains _num_duration_cols
def _init_num_duration_cols():
    # read the file to get _num_duration_cols
    try:
        with open(NUM_DURATION_COLS_FILE, "r") as file:
            num_duration_cols = int(file.read().strip())
        # return if successful
        return num_duration_cols

    # handle errors for reading file
    except FileNotFoundError:
        print(f"Error: File {NUM_DURATION_COLS_FILE} not found.")
        raise ValueError(f"Error: File {NUM_DURATION_COLS_FILE} not found.")
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


_INCLUDE_ALL_TOTALS_METRICS = True

'''
========================
    Public Functions
========================
'''


def include_all_totals_metrics(status=True):
    if not isinstance(status, bool):
        raise ValueError(
            f"status ({status}) must be of type bool but was type {type(status)}.")
    global _INCLUDE_ALL_TOTALS_METRICS
    _INCLUDE_ALL_TOTALS_METRICS = status

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
            updated_num = input(
                "How many partial duration columns would you like?")
            set_num_duration_cols(updated_num)

    # initialize num_duration_cols by writing to its specified file
    # convert num back to a string so that it can be written to the file
    num = str(num)
    with open(NUM_DURATION_COLS_FILE, "w") as file:
        file.write(num)


# get a dict of metrics by type (all, static, non_static)
def GET_METRICS():
    # metrics
    all_metrics = [
        "cpu_usage",
        "mem_usage",
        "cpu_request",
        "mem_request",
        "transmitted_packets",
        "received_packets",
        "transmitted_bandwidth",
        "received_bandwidth"
    ]
    static_metrics = ["cpu_request", "mem_request"]
    non_static_metrics = [
        metric for metric in all_metrics if metric not in static_metrics]

    totals_metrics = []
    if _INCLUDE_ALL_TOTALS_METRICS:
        totals_metrics = non_static_metrics
    else:
        totals_metrics = ['cpu_usage', 'mem_usage']
    metrics_dict = {
        "all": all_metrics,
        "static": static_metrics,
        "non_static": non_static_metrics,
        "totals": totals_metrics
    }
    return metrics_dict


# Note: get_duration_cols must be a function that is called only during initialization or later, otherwise if a user changes the number of duration columns in work_flow, it won't actually update until next run
# get a dict of duration column information (num_cols, col_names, total_col)
def GET_DURATION_COLS():
    # duration columns
    # Note: assumes duration columns are in the form "duration_t{N}" where {N} is
    # an int from 1 to _num_duration_cols inclusive
    num_duration_cols = _init_num_duration_cols()
    duration_col_names = ["duration_t" +
                          str(num) for num in range(1, (num_duration_cols+1))]
    duration_col_total = "runtime"  # from phase_2

    duration_cols_dict = {
        "num_cols": num_duration_cols,
        "col_names": duration_col_names,
        "total_col": duration_col_total
    }
    return duration_cols_dict


# get a dict of column names by type (static, totals, by_time, all)
def GET_COL_NAMES():
    metrics = GET_METRICS()
    num_duration_cols = GET_DURATION_COLS()['num_cols']
    # column names
    static_col_names = metrics['static']
    totals_col_names = [name + "_total" for name in metrics['totals']]
    col_names_by_time = _init_col_names_by_time(
        num_duration_cols, metrics['non_static'])
    all_col_names = static_col_names + totals_col_names + col_names_by_time

    col_names_dict = {
        "static": static_col_names,
        "totals": totals_col_names,
        "by_time": col_names_by_time,
        "all": all_col_names
    }
    return col_names_dict


# get a dict of id columns by type (ensemble, run)
def GET_ID_COLS():
    id_cols_dict = {
        "ensemble": "ensemble_uuid",
        "run": "run_uuid"
    }
    return id_cols_dict
