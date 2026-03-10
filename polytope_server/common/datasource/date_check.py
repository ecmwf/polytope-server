import re
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from ..exceptions import ServerError


class DateError(Exception):
    """Custom exception for date-related errors."""

    pass


def date_check(date: str, allowed_values: list[str]):
    """
    Process special match rules for DATE constraints.

    :param date: Date to check, can be a single, list or range of dates (Mars date format)
    :param allowed_values: List of rules. All rules must be the same style:

        Old style (e.g. ">30d", "<40d"):
            Each date must satisfy ALL rules (AND logic).

        New style – Mars date strings (e.g. "-1/-5/-10", "-1/to/-20", "-4/to/-20/by/4"):
            Each individual date must match AT LEAST ONE rule (OR logic).
    """
    if not isinstance(allowed_values, list):
        raise ServerError("Allowed values must be a list")

    if not allowed_values:
        return True

    if all(map(is_old_style_rule, allowed_values)):
        # Old-style: every rule must pass
        for rule in allowed_values:
            if not date_check_single_rule(date, rule):
                return False
        return True

    # New-style Mars date rules: each user date must match at least one rule
    user_dates = expand_mars_dates(date)
    for user_date in user_dates:
        if not any(date_in_mars_rule(user_date, rule) for rule in allowed_values):
            raise DateError(f"Date {user_date} does not match any allowed date rule: {allowed_values}")

    return True


def check_single_date(date, offset, offset_fmted, after=False):
    # Date is relative (0 = now, -1 = one day ago)
    if str(date)[0] == "0" or str(date)[0] == "-":
        date_offset = int(date)
        dt = datetime.today() + timedelta(days=date_offset)

        if after and dt > offset:
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
    if after and dt > offset:
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


def is_old_style_rule(rule: str) -> bool:
    """Returns True if rule is old-style (starts with > or <)."""
    return rule.strip()[0] in (">", "<")


def parse_mars_date_token(token: str) -> datetime:
    """Parse a single Mars date token to a datetime.

    Supports:
    - Relative dates: 0 (today), -1 (yesterday), -10, etc.
    - Absolute YYYYMMDD: 20250125
    - Absolute YYYY-MM-DD: 2023-04-23
    """
    token = token.strip()
    if not token:
        raise DateError("Empty date token")
    # Relative date: starts with '-' or '0' (matches existing check_single_date convention)
    if token[0] == "-" or token[0] == "0":
        try:
            offset = int(token)
            return datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=offset)
        except ValueError:
            pass
    # ISO format YYYY-MM-DD
    try:
        return datetime.strptime(token, "%Y-%m-%d")
    except ValueError:
        pass
    # YYYYMMDD format
    try:
        return datetime.strptime(token, "%Y%m%d")
    except ValueError:
        raise DateError(f"Invalid Mars date token: {token!r}")


def expand_mars_dates(date_str: str) -> list:
    """Expand a Mars date string to a list of date objects.

    Handles single dates, lists, ranges, and ranges with step.

    Examples:
        "-1"                             -> [date(-1)]
        "-1/-5/-10"                      -> [date(-1), date(-5), date(-10)]
        "-1/to/-20"                      -> [date(-1), date(-2), ..., date(-20)]
        "-4/to/-20/by/4"                 -> [date(-4), date(-8), date(-12), date(-16), date(-20)]
        "20250125/-5/2023-04-23"         -> [date(20250125), date(-5), date(2023-04-23)]
        "2024-02-21/to/2025-03-01/by/10" -> dates every 10 days across the range
    """
    parts = date_str.split("/")

    # Range syntax: second element is 'to'
    if len(parts) >= 3 and parts[1].strip().lower() == "to":
        start_d = parse_mars_date_token(parts[0]).date()
        end_d = parse_mars_date_token(parts[2]).date()
        step = 1
        if len(parts) == 5:
            if parts[3].strip().lower() != "by":
                raise DateError(f"Invalid Mars date string: {date_str!r}")
            step = abs(int(parts[4].strip()))
        elif len(parts) != 3:
            raise DateError(f"Invalid Mars date string: {date_str!r}")

        dates = []
        if start_d <= end_d:
            current = start_d
            while current <= end_d:
                dates.append(current)
                current += timedelta(days=step)
        else:
            current = start_d
            while current >= end_d:
                dates.append(current)
                current -= timedelta(days=step)
        return dates

    # List or single date
    return [parse_mars_date_token(p).date() for p in parts]


def date_in_mars_rule(date_d, rule_str: str) -> bool:
    """Check whether a single date (a date object) is covered by a Mars date rule string.

    The rule string follows the same Mars date syntax:
    - Single:     '-1', '20250125', '2023-04-23'
    - List:       '-1/-5/-10', '20250125/-5/2023-04-23'
    - Range:      '-1/to/-20', '2024-02-21/to/2025-03-01'
    - Range+step: '-4/to/-20/by/4', '2024-02-21/to/2025-03-01/by/10'
    """
    parts = rule_str.split("/")

    # Range syntax
    if len(parts) >= 3 and parts[1].strip().lower() == "to":
        start_d = parse_mars_date_token(parts[0]).date()
        end_d = parse_mars_date_token(parts[2]).date()
        step = 1
        if len(parts) == 5:
            if parts[3].strip().lower() != "by":
                raise DateError(f"Invalid Mars date rule: {rule_str!r}")
            step = abs(int(parts[4].strip()))
        elif len(parts) != 3:
            raise DateError(f"Invalid Mars date rule: {rule_str!r}")

        min_d = min(start_d, end_d)
        max_d = max(start_d, end_d)
        if not (min_d <= date_d <= max_d):
            return False
        # Date must fall on a step boundary from start
        return abs((date_d - start_d).days) % step == 0

    # List or single: date must equal one of the listed tokens
    for part in parts:
        if parse_mars_date_token(part).date() == date_d:
            return True
    return False


def date_check_single_rule(date, allowed_values: str):
    """
    Process special match rules for DATE constraints (old-style rules only).

    :param date: Date to check, can be a string or list of strings
    :param allowed_values: Allowed values for the date in the format >1d, <2d, >1m, <2h, r"(\\d+)([dhm])".
    """
    # if type of date is list
    if isinstance(date, list):
        date = "/".join(date)
    date = str(date)

    # Parse allowed values
    comp = allowed_values[0]
    offset = allowed_values[1:].strip()
    if comp == "<":
        after = False
    elif comp == ">":
        after = True
    else:
        raise ServerError(f"Invalid date comparison {comp}, expected < or >")
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
