import re
from datetime import datetime, timedelta
import pandas as pd


# returns datetime of time if it matches one of the format strings, otherwise none
def _try_strptime(time: str, format_strings: list[str]) -> datetime | None:
    for format_str in format_strings:
        try:
            time = datetime.strptime(time, format_str)
            return time
        except ValueError:
            continue
    return None


# convert a time string into a datetime object
def datetime_ify(time: str | int | float | pd.Timestamp | datetime) -> datetime:
    # handle if time is already of type pandas datetime or actual datetime
    if isinstance(time, pd.Timestamp):
        return time.to_pydatetime(warn=False)
    if isinstance(time, datetime):
        return time
    # handle if time is a float (seconds since the epoch: 01/01/1970)
    if isinstance(time, (float, int)):
        return datetime.fromtimestamp(time)
    type_error_msg = f"time was type {type(time)}, not one of the expected formats:\
          str | pd.Timestamp | float | datetime "
    assert isinstance(time, str), type_error_msg
    # get time as datetime object. Time format should be one of three patterns.
    # get time down to the second, no decimal seconds.
    time = time[0:time.find(".")]
    # define expected format strings
    format_strings = ["%Y-%m-%dT%H:%M:%S",
                      "%m/%d/%Y, %H:%M:%S", '%Y-%m-%d %H:%M:%S']
    time_dt = _try_strptime(time, format_strings)
    # return the datetime or raise a value error
    if time_dt is not None:
        return time_dt
    raise ValueError(
        "time did not match one of the expected time string formats")


# given a timedelta, get it in the form 2d4h12m30s for use with querying
def delta_to_time_str(delta: timedelta) -> str:
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_str = f"{days}d{hours}h{minutes}m{seconds}s"
    return time_str


# given a string in the form 5w3d6h30m5s, save the times to a dict accesible
# by the unit as their key. The int times can be any length (500m160s is allowed)
# works given as many or few of the time units. (e.g. 12h also works and sets everything but h to None)
def time_str_to_delta(time_str: str) -> timedelta:
    # define regex pattern (groups by optional int+unit but only keeps the int)
    pattern = r"(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?"
    feedback = re.search(pattern, time_str)

    # save time variables (if not in time_str they will be set to None)
    w, d, h, m, s = feedback.groups()
    # put time variables into a dictionary
    time_dict = {
        'weeks': w,
        'days': d,
        'hours': h,
        'minutes': m,
        'seconds': s
    }

    # get rid of null values in time_dict
    time_dict = {
        unit: float(value) for unit, value
        in time_dict.items() if value is not None
    }
    # create new datetime timedelta to represent the time
    # and pass in parameters as values from time_dict
    time_delta = timedelta(**time_dict)

    return time_delta


# given a start (datetime object) of a run and duration in seconds,
# return the offset of the end of the run from the current time
def calculate_offset(start: datetime, duration: int) -> str:
    # check for proper inputs
    assert isinstance(start, datetime), "start must be a datetime object"
    try:
        duration = float(duration)
    except ValueError:
        # pylint: disable=raise-missing-from
        raise ValueError("duration must be a float or int")

    # calculate offset from current time by finding end time and subtracting now from end
    end = start + timedelta(seconds=duration)
    offset_delta = datetime.now() - end
    # convert offset from timedelta to time string
    offset = delta_to_time_str(offset_delta)

    return offset


# given an end_time (datetime) and an offset (string) (e.g. "12h5m30s"),
# return a new datetime object offset away from the end_time
def find_time_from_offset(end: datetime, offset: str) -> datetime:
    # get the offset in a usable form (a timedelta)
    time_offset = time_str_to_delta(offset)
    # return the start time
    return end-time_offset
