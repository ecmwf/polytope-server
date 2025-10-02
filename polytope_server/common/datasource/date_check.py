import re
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta


class DateError(Exception):
    """Custom exception for date-related errors."""

    pass


def check_single_date(date, offset, offset_fmted, after=False):
    # Date is relative (0 = now, -1 = one day ago)
    if str(date)[0] == "0" or str(date)[0] == "-":
        date_offset = int(date)
        dt = datetime.today() + timedelta(days=date_offset)

        if after and dt >= offset:
            raise DateError("Date is too recent, expected < {}".format(offset_fmted))
        elif not after and dt < offset:
            raise DateError("Date is too old, expected > {}".format(offset_fmted))
        else:
            return

    # Absolute date YYYMMDD
    try:
        dt = datetime.strptime(date, "%Y%m%d")
    except ValueError:
        raise DateError("Invalid date, expected real date in YYYYMMDD format")
    if after and dt >= offset:
        raise DateError("Date is too recent, expected < {}".format(offset_fmted))
    elif not after and dt < offset:
        raise DateError("Date is too old, expected > {}".format(offset_fmted))
    else:
        return


def parse_relativedelta(time_str):
    pattern = r"(\d+)([dhm])"
    time_dict = {"d": 0, "h": 0, "m": 0}
    matches = re.findall(pattern, time_str)

    for value, unit in matches:
        if unit == "d":
            time_dict["d"] += int(value)
        elif unit == "h":
            time_dict["h"] += int(value)
        elif unit == "m":
            time_dict["m"] += int(value)

    return relativedelta(days=time_dict["d"], hours=time_dict["h"], minutes=time_dict["m"])


def date_check(date, allowed_values: list):
    """
    Process special match rules for DATE constraints

    :param date: Date to check, can be a string or list of strings
    :param allowed_values: List of allowed values for the date in the format >1d, <2d, >1m, <2h, r"(\\d+)([dhm])".
    """
    if not isinstance(allowed_values, list):
        raise DateError("Allowed values must be a list")

    for allowed in allowed_values:
        try:
            if date_check_single_rule(date, allowed):
                return True
        except DateError:
            continue

    raise DateError(f"Date {date} does not match any of the allowed values: {allowed_values}")


def date_check_single_rule(date, allowed_values: str):
    """
    Process special match rules for DATE constraints

    :param date: Date to check, can be a string or list of strings
    :param allowed_values: Allowed values for the date in the format >1d, <2d, >1m, <2h, r"(\\d+)([dhm])".
    """
    # if type of date is list
    if isinstance(date, list):
        date = "/".join(date)
    date = str(date)

    # Parse allowed values
    comp, offset = allowed_values.split(" ", 1)
    if comp == "<":
        after = False
    elif comp == ">":
        after = True
    else:
        raise DateError("Invalid date comparison")
    now = datetime.today()
    offset = now - parse_relativedelta(offset)
    offset_fmted = offset.strftime("%Y%m%d")

    split = date.split("/")

    # YYYYMMDD
    if len(split) == 1:
        check_single_date(split[0], offset, offset_fmted, after)
        return True

    # YYYYMMDD/to/YYYYMMDD -- check end and start date
    # YYYYMMDD/to/YYYYMMDD/by/N -- check end and start date
    if len(split) == 3 or len(split) == 5:
        if split[1].casefold() == "to".casefold():
            if len(split) == 5 and split[3].casefold() != "by".casefold():
                raise DateError("Invalid date range")

            check_single_date(split[0], offset, offset_fmted, after)
            check_single_date(split[2], offset, offset_fmted, after)
            return True

    # YYYYMMDD/YYYYMMDD/YYYYMMDD/... -- check each date
    for s in split:
        check_single_date(s, offset, offset_fmted, after)

    return True
