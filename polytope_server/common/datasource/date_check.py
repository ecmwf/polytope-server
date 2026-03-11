import re
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from ..exceptions import ServerError


class DateError(Exception):
    """Custom exception for date-related errors."""

    pass


def date_check(date: str, rules: list[str]):
    """
    Process special match rules for DATE constraints.

    :param date: Date to check, can be a single, list or range of dates (Mars date format)
    :param rules: List of rules. All rules must be the same style:

        - Comparative (e.g. ">30d", "<40d"):
            Each date must satisfy ALL rules (AND logic).

        - Mars date strings (e.g. "-1/-5/-10", "-1/to/-20" but without "by" in rules):
            Each user date or date range must match AT LEAST ONE rule (OR logic).
    """
    if not isinstance(rules, list):
        raise ServerError("Allowed values must be a list")

    if not rules:
        return True

    are_comparative_rules = set(is_comparative_rule(rule) for rule in rules)
    if all(are_comparative_rules):
        # Comparative: every rule must pass
        for rule in rules:
            if not date_check_comparative_rule(date, rule):
                return False
        return True

    if any(are_comparative_rules):
        raise ServerError("Cannot mix comparative and new-style date rules in a single match.")

    # New-style Mars date rules.
    date_parts = date.split("/")
    if len(date_parts) >= 3 and date_parts[1].strip().lower() == "to":
        # Range: both boundaries must be covered by the same rule.
        start_d = parse_mars_date_token(date_parts[0]).date()
        end_d = parse_mars_date_token(date_parts[2]).date()
        if len(date_parts) == 5:
            if date_parts[3].strip().lower() != "by":
                raise DateError(f"Invalid Mars date string: {date!r}")
        elif len(date_parts) != 3:
            raise DateError(f"Invalid Mars date string: {date!r}")
        if not any(date_in_mars_rule(start_d, rule) and date_in_mars_rule(end_d, rule) for rule in rules):
            raise DateError(
                f"Date range {start_d} to {end_d} is not fully covered by any single allowed date rule: {rules}"
            )
    else:
        # List or single: each date must match at least one rule (OR logic).
        for user_date in [parse_mars_date_token(p).date() for p in date_parts]:
            if not any(date_in_mars_rule(user_date, rule) for rule in rules):
                raise DateError(f"Date {user_date} does not match any allowed date rule: {rules}")

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


def is_comparative_rule(rule: str) -> bool:
    """Returns True if rule is comparative (starts with > or <)."""
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

    Handles single dates, lists, ranges, and ranges with "by".

    Examples:
        "-1"                             -> [date(-1)]
        "-1/-5/-10"                      -> [date(-1), date(-5), date(-10)]
        "-1/to/-20"                      -> [date(-1), date(-2), ..., date(-20)]
        "-4/to/-20/by/4"                 -> [date(-4), date(-8), date(-12), date(-16), date(-20)]
        "20250125/-5/2023-04-23"         -> [date(20250125), date(-5), date(2023-04-23)]
        "2024-02-21/to/2025-03-01/by/10" -> dates every 10 days across the range
    """
    date_parts = date_str.split("/")

    # Range syntax: second element is 'to'
    if len(date_parts) >= 3 and date_parts[1].strip().lower() == "to":
        start_d = parse_mars_date_token(date_parts[0]).date()
        end_d = parse_mars_date_token(date_parts[2]).date()
        by = 1
        if len(date_parts) == 5:
            if date_parts[3].strip().lower() != "by":
                raise DateError(f"Invalid Mars date string: {date_str!r}")
            by = abs(int(date_parts[4].strip()))
            if by == 0:
                raise DateError(f"By value cannot be zero in Mars date string: {date_str!r}")
        elif len(date_parts) != 3:
            raise DateError(f"Invalid Mars date string: {date_str!r}")

        dates = []
        if start_d <= end_d:
            current = start_d
            while current <= end_d:
                dates.append(current)
                current += timedelta(days=by)
        else:
            current = start_d
            while current >= end_d:
                dates.append(current)
                current -= timedelta(days=by)
        return dates

    # List or single date
    return [parse_mars_date_token(p).date() for p in date_parts]


def date_in_mars_rule(date_d, rule_str: str) -> bool:
    """Check whether a single date (a date object) is covered by a Mars date rule string.

    The rule string follows the same Mars date syntax:
    - Single:  '-1', '20250125', '2023-04-23'
    - List:    '-1/-5/-10', '20250125/-5/2023-04-23'
    - Range:   '-1/to/-20', '2024-02-21/to/2025-03-01'

    Note: 'by' is not supported in rules. Use it only in user-supplied dates.
    """
    rule_parts = rule_str.split("/")

    # Range syntax
    if len(rule_parts) >= 3 and rule_parts[1].strip().lower() == "to":
        if len(rule_parts) != 3:
            raise DateError(f"'by' is not supported in date rules: {rule_str!r}")
        start_d = parse_mars_date_token(rule_parts[0]).date()
        end_d = parse_mars_date_token(rule_parts[2]).date()
        return min(start_d, end_d) <= date_d <= max(start_d, end_d)

    # List or single: date must equal one of the listed tokens
    for part in rule_parts:
        if parse_mars_date_token(part).date() == date_d:
            return True
    return False


def date_check_comparative_rule(date, comp_rule: str):
    """
    Process special match rules for DATE constraints (comparative rules only).

    :param date: Date to check, can be a string or list of strings
    :param comp_rule: Comparative rule for the date in the format >1d, <2d, >1m, <2h, r"(\\d+)([dhm])".

    """
    # if type of date is list
    if isinstance(date, list):
        date = "/".join(date)
    date = str(date)

    # Parse allowed values
    comp = comp_rule[0]
    offset = comp_rule[1:].strip()
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
