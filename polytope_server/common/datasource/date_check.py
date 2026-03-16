import re
from collections.abc import Sequence
from datetime import date, datetime, timedelta

from dateutil.relativedelta import relativedelta

from ..exceptions import ServerError


class DateError(Exception):
    """Custom exception for date-related errors."""

    pass


def validate_date_match(date: str, rules: Sequence[str]) -> None:
    """
    Check a Mars-format date string against a list of allowed date rules.

    :param date: Date to check. Accepts a single date, a slash-separated list, or a
        range in Mars date format (e.g. ``-1``, ``-1/-5/-10``,
        ``20250101/to/20250131``, ``20250101/to/20250131/by/7``).
    :param rules: List or tuple of rules. All rules must be the same style:

        - Comparative (e.g. ``>30d``, ``<40d``, ``>1h``, ``<2m``):
            Each date must satisfy ALL rules (AND logic).

        - Mars date strings (e.g. ``-1/-5/-10``, ``-1/to/-20``; ``by`` is not
          supported in rules):
            Each user date or date range must be fully covered by AT LEAST ONE
            rule (OR logic).

    :returns: ``None``. Returns normally when the date passes all checks.
    :raises ServerError: If ``rules`` is not a list-like sequence, or if comparative
        and Mars-style rules are mixed.
    :raises DateError: If the date does not satisfy the rules.
    """
    if not isinstance(rules, Sequence) or isinstance(rules, (str, bytes)):
        raise ServerError("Allowed values must be a list")

    if not rules:
        return

    are_comparative_rules = set(_is_comparative_rule(rule) for rule in rules)
    if all(are_comparative_rules):
        # Comparative: every rule must pass
        for rule in rules:
            validate_comparative_date_rule(date, rule)
        return

    if any(are_comparative_rules):
        raise ServerError("Cannot mix comparative and new-style date rules in a single match.")

    # New-style Mars date rules.
    date_parts = date.split("/")
    if len(date_parts) >= 3 and date_parts[1].strip().lower() == "to":
        # Range: both boundaries must be covered by the same *range* rule.
        # A list rule cannot cover a continuous range (intermediate dates would be unaccounted for).
        start_d = parse_mars_date_token(date_parts[0]).date()
        end_d = parse_mars_date_token(date_parts[2]).date()
        if len(date_parts) == 5:
            if date_parts[3].strip().lower() != "by":
                raise DateError(f"Invalid Mars date string: {date!r}")
        elif len(date_parts) != 3:
            raise DateError(f"Invalid Mars date string: {date!r}")
        if not any(
            _is_mars_range_rule(rule) and date_in_mars_rule(start_d, rule) and date_in_mars_rule(end_d, rule)
            for rule in rules
        ):
            raise DateError(
                f"Date range {start_d} to {end_d} is not fully covered by any single allowed date rule: {rules}"
            )
    else:
        # List or single: each date must match at least one rule (OR logic).
        for user_date in [parse_mars_date_token(p).date() for p in date_parts]:
            if not any(date_in_mars_rule(user_date, rule) for rule in rules):
                raise DateError(f"Date {user_date} does not match any allowed date rule: {rules}")

    return


def _check_single_date_comparative_rule(date: str, offset: datetime, offset_fmted: str, after: bool = False) -> None:
    """
    Check that a single Mars date token satisfies a comparative offset constraint.

    :param date: A single date token — either a relative integer string (e.g. ``"0"``,
        ``"-1"``) or an absolute date in ``YYYYMMDD`` format.
    :param offset: The cutoff ``datetime`` to compare against.
    :param offset_fmted: Human-readable string of ``offset`` used in error messages.
    :param after: If ``True``, the date must be *before* ``offset`` (``<`` semantics);
        if ``False``, the date must be *after* ``offset`` (``>`` semantics).
    :raises DateError: If the date falls outside the allowed range or is invalid.
    """
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

    # Absolute date YYYYMMDD
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


def _parse_relativedelta(time_str: str) -> relativedelta:
    """
    Parse a duration string into a :class:`relativedelta`.

    Supports days (``d``), hours (``h``), and minutes (``m``), which may be
    combined (e.g. ``"1d2h30m"``).

    :param time_str: Duration string such as ``"30d"``, ``"2h"``, ``"1d12h"``.
    :returns: A :class:`relativedelta` representing the parsed duration.
    """
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


def _is_comparative_rule(rule: str) -> bool:
    """Returns True if rule is comparative (starts with > or <)."""
    return rule.strip()[0] in (">", "<")


def _is_mars_range_rule(rule_str: str) -> bool:
    """Returns True if the Mars rule is a range (A/to/B)."""
    parts = rule_str.split("/")
    return len(parts) >= 3 and parts[1].strip().lower() == "to"


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


def date_in_mars_rule(date_d: date, rule_str: str) -> bool:
    """Check whether a single date is covered by a Mars date rule string.

    The rule string follows the same Mars date syntax:

    - Single:  ``'-1'``, ``'20250125'``, ``'2023-04-23'``
    - List:    ``'-1/-5/-10'``, ``'20250125/-5/2023-04-23'``
    - Range:   ``'-1/to/-20'``, ``'2024-02-21/to/2025-03-01'``

    :param date_d: The :class:`~datetime.date` to test.
    :param rule_str: A Mars-format allowed-date rule string. ``by`` is not
        supported in rules — use it only in user-supplied date strings.
    :returns: ``True`` if ``date_d`` falls within the rule.
    :raises ServerError: If the rule contains ``by`` or is otherwise malformed.
    :raises DateError: If the date does not match the rule.
    """
    rule_parts = rule_str.split("/")

    # Range syntax
    if len(rule_parts) >= 3 and rule_parts[1].strip().lower() == "to":
        if len(rule_parts) != 3:
            raise ServerError(f"'by' is not supported in date rules: {rule_str!r}")
        try:
            start_d = parse_mars_date_token(rule_parts[0]).date()
            end_d = parse_mars_date_token(rule_parts[2]).date()
        except DateError as e:
            raise ServerError(f"Invalid date token in rule {rule_str!r}: {e}") from e
        return min(start_d, end_d) <= date_d <= max(start_d, end_d)

    # List or single: validate all tokens first (malformed rule = ServerError), then match
    try:
        parsed_rule_parts = [parse_mars_date_token(part).date() for part in rule_parts]
    except DateError as e:
        raise ServerError(f"Invalid date token in rule {rule_str!r}: {e}") from e
    return date_d in parsed_rule_parts


def validate_comparative_date_rule(date: str | list[str], comp_rule: str) -> None:
    """
    Check a date (or list/range of dates) against a single comparative rule.

    :param date: Date to check. Either a single date string or a list of date strings,
        each in Mars format (relative integer, ``YYYYMMDD``, or ``YYYYMMDD/to/YYYYMMDD``).
    :param comp_rule: A comparative rule in the form ``>Nd``, ``<Nd``, ``>Nh``, or
        ``<Nm`` where ``N`` is a positive integer and the suffix is ``d`` (days),
        ``h`` (hours), or ``m`` (minutes). For example: ``">30d"``, ``"<2h"``.
    :returns: ``None``. Returns normally if all dates in ``date`` satisfy the rule.
    :raises DateError: If a date is invalid or falls outside the allowed range.
    :raises ServerError: If the comparison operator is not ``<`` or ``>``.
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
    offset = now - _parse_relativedelta(offset)
    offset_fmted = offset.strftime("%Y%m%d")

    split = date.split("/")

    # YYYYMMDD
    if len(split) == 1:
        _check_single_date_comparative_rule(split[0], offset, offset_fmted, after)
        return

    # YYYYMMDD/to/YYYYMMDD -- check end and start date
    # YYYYMMDD/to/YYYYMMDD/by/N -- check end and start date
    if len(split) == 3 or len(split) == 5:
        if split[1].casefold() == "to".casefold():
            if len(split) == 5 and split[3].casefold() != "by".casefold():
                raise DateError("Invalid date range")

            _check_single_date_comparative_rule(split[0], offset, offset_fmted, after)
            _check_single_date_comparative_rule(split[2], offset, offset_fmted, after)
            return

    # YYYYMMDD/YYYYMMDD/YYYYMMDD/... -- check each date
    for s in split:
        _check_single_date_comparative_rule(s, offset, offset_fmted, after)

    return
