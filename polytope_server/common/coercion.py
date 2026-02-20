import copy
import re
from datetime import datetime, timedelta
from typing import Any, Dict

from . import config as polytope_config


class CoercionError(Exception):
    pass


default_config = {
    "allow_ranges": ["number", "step", "date", "time"],
    "allow_lists": ["class", "stream", "type", "expver", "param", "number", "date", "step", "time"],
    "number_allow_zero": False,
}

config = polytope_config.global_config.get("coercion", {}) if polytope_config.global_config else {}
allow_ranges = config.get("allow_ranges", default_config["allow_ranges"])
allow_lists = config.get("allow_lists", default_config["allow_lists"])
number_allow_zero = config.get("number_allow_zero", default_config["number_allow_zero"])


def coerce(request: Dict[str, Any] | str | int | None) -> Dict[str, Any]:
    if not isinstance(request, dict):
        return {"data": request}
    request = copy.deepcopy(request)
    errors = ""
    for key, value in request.items():
        try:
            request[key] = coerce_value(key, value)
        except CoercionError as e:
            errors += f"\n {str(e.args[0])} for key '{key}' with value '{value}'"
            continue
        # check that lists don't have duplicates
        if isinstance(request[key], list):
            # find duplicates and raise an error
            duplicates = set(x for x in request[key] if request[key].count(x) > 1)
            if duplicates:
                errors += f"\nDuplicate values found in list for key '{key}': {duplicates}"
    if errors:
        raise CoercionError(f"Errors in request:{errors}")
    return request


def coerce_value(key: str, value: Any) -> Any:
    if key in coercer:
        coercer_func = coercer.get(key, None)

        if coercer_func is None:
            return value

        if isinstance(value, list):
            # Coerce each item in the list
            coerced_values = [coerce_value(key, v) for v in value]
            return coerced_values
        elif isinstance(value, str):
            if "/to/" in value and key in allow_ranges:
                # Handle ranges with possible "/by/" suffix
                start_value, rest = value.split("/to/", 1)
                if not rest:
                    raise CoercionError(f"Invalid range format for key {key}.")

                if "/by/" in rest:
                    end_value, suffix = rest.split("/by/", 1)
                    suffix = "/by/" + suffix  # Add back the '/by/'
                else:
                    end_value = rest
                    suffix = ""

                # Coerce start_value and end_value
                start_coerced = coercer_func(start_value)
                end_coerced = coercer_func(end_value)

                return f"{start_coerced}/to/{end_coerced}{suffix}"
            elif "/" in value and key in allow_lists:
                # Handle lists
                coerced_values = [coercer_func(v) for v in value.split("/")]
                return coerced_values
            else:
                # Single value
                return coercer_func(value)
        else:  # not list or string
            return coercer_func(value)
    else:
        if isinstance(value, list):
            # Join list into '/' separated string
            coerced_values = [str(v) for v in value]
            return coerced_values
        else:
            return value


def coerce_date(value: Any) -> str:
    try:
        # Attempt to convert the value to an integer
        int_value = int(value)
        if int_value > 0:
            # Positive integers are assumed to be dates in YYYYMMDD format
            date_str = str(int_value)
            try:
                datetime.strptime(date_str, "%Y%m%d")
                return date_str
            except ValueError:
                raise CoercionError("Invalid date format, expected YYYYMMDD or YYYY-MM-DD.")
        else:
            # Zero or negative integers represent relative days from today
            target_date = datetime.today() + timedelta(days=int_value)
            return target_date.strftime("%Y%m%d")
    except (ValueError, TypeError):
        # The value is not an integer or cannot be converted to an integer
        pass

    if isinstance(value, str):
        value_stripped = value.strip()
        # Try parsing as YYYYMMDD
        try:
            datetime.strptime(value_stripped, "%Y%m%d")
            return value_stripped
        except ValueError:
            # Try parsing as YYYY-MM-DD
            try:
                date_obj = datetime.strptime(value_stripped, "%Y-%m-%d")
                return date_obj.strftime("%Y%m%d")
            except ValueError:
                raise CoercionError("Invalid date format, expected YYYYMMDD or YYYY-MM-DD.")
    else:
        raise CoercionError("Invalid date format, expected YYYYMMDD or YYYY-MM-DD.")


def coerce_step(value: Any) -> str:
    if isinstance(value, int):
        if value < 0:
            raise CoercionError("Step must be greater than or equal to 0.")
        else:
            return str(value)
    elif isinstance(value, str):
        if _is_valid_step(value):
            return value
        # check step ranges
        step_range_pattern = r"^(.*)-(.*)$"
        step_match = re.match(step_range_pattern, value)
        if step_match and _is_valid_step(step_match.group(1)) and _is_valid_step(step_match.group(2)):
            return value
        raise CoercionError(
            "Invalid step format, expected integer, steps with units (e.g., '1h', '30m', '1h30m', '2d', '30s'),"
            + " or a range of these formats (e.g., '1h-3')."
        )
    else:
        raise CoercionError("Invalid type, expected integer or string.")


def _is_valid_step(value: str) -> bool:
    """
    Checks if the single step value (not range) is valid. Valid formats include:
    - Integer (e.g., "6")
    - Step with time units (e.g., "1h", "30m", "1h30m", "30s", "3d" etc.)
    """
    units = ["d", "h", "m", "s"]
    pattern = r"^\d+" + r"?".join(rf"(\d*{unit})" for unit in units) + r"?$"
    # pattern = r"^\d+(\d*d)?(\d*h)?(\d*m)?(\d*s)?$"  # left for readability, above expands to this
    return re.match(pattern, value) is not None


def coerce_number(value: Any) -> str:
    min_value = 0 if number_allow_zero else 1
    if isinstance(value, int):
        if value < min_value:
            raise CoercionError(f"Number must be >= {min_value}.")
        else:
            return str(value)
    elif isinstance(value, str):
        if not value.isdigit() or int(value) < min_value:
            raise CoercionError(f"Number must be >= {min_value}.")
        return value
    else:
        raise CoercionError("Invalid type, expected integer or string.")


def coerce_param(value: Any) -> str:
    if isinstance(value, int):
        return str(value)
    elif isinstance(value, str):
        return value
    else:
        raise CoercionError("Invalid param type, expected integer or string.")


def coerce_time(value: Any) -> str:
    if isinstance(value, int):
        if value < 0:
            raise CoercionError("Invalid time format, expected HHMM or HH greater than zero.")
        elif value < 24:
            # Treat as hour with minute=0
            hour = value
            minute = 0
        elif 100 <= value <= 2359:
            # Possible HHMM format
            hour = value // 100
            minute = value % 100
        else:
            raise CoercionError("Invalid time format, expected HHMM or HH.")
    elif isinstance(value, str):
        value_stripped = value.strip()
        # Check for colon-separated time (e.g., "12:00")
        if ":" in value_stripped:
            parts = value_stripped.split(":")
            if len(parts) != 2:
                raise CoercionError("Invalid time format, expected HHMM or HH.")
            hour_str, minute_str = parts
            if not (hour_str.isdigit() and minute_str.isdigit()):
                raise CoercionError("Invalid time format, expected HHMM or HH.")
            hour = int(hour_str)
            minute = int(minute_str)
        else:
            if value_stripped.isdigit():
                num_digits = len(value_stripped)
                if num_digits == 4:
                    # Format is "HHMM"
                    hour = int(value_stripped[:2])
                    minute = int(value_stripped[2:])
                elif num_digits <= 2:
                    # Format is "H" or "HH"
                    hour = int(value_stripped)
                    minute = 0
                else:
                    raise CoercionError("Invalid time format, expected HHMM or HH.")
            else:
                raise CoercionError("Invalid time format, expected HHMM or HH.")
    else:
        raise CoercionError("Invalid type for time, expected string or integer.")

    # Validate hour and minute
    if not (0 <= hour <= 23):
        raise CoercionError("Invalid time format, expected HHMM or HH.")
    if not (0 <= minute <= 59):
        raise CoercionError("Invalid time format, expected HHMM or HH.")
    if minute != 0:
        raise CoercionError("Invalid time format, expected HHMM or HH.")

    # Format time as HHMM
    time_str = f"{hour:02d}{minute:02d}"
    return time_str


def coerce_expver(value: Any) -> str:
    # Integers accepted, converted to 4-length strings
    if isinstance(value, int):
        if 0 <= value <= 9999:
            return f"{value:0>4d}"
        else:
            raise CoercionError("expver integer must be between 0 and 9999 inclusive.")

    # Strings accepted if they are convertible to integer or exactly 4 characters long
    elif isinstance(value, str):
        if value.isdigit():
            int_value = int(value.lstrip("0") or "0")
            if 0 <= int_value <= 9999:
                return f"{int_value:0>4d}"
            else:
                raise CoercionError("expver integer string must represent a number between 0 and 9999 inclusive.")
        elif len(value) == 4:
            return value
        else:
            raise CoercionError("expver string length must be 4 characters exactly.")

    else:
        raise CoercionError("expver must be an integer or a string.")


def coerce_ignore_cases(value: Any) -> str:
    return value.lower()


coercer = {
    "date": coerce_date,
    "step": coerce_step,
    "number": coerce_number,
    "param": coerce_param,
    "time": coerce_time,
    "expver": coerce_expver,
    "model": coerce_ignore_cases,
    "experiment": coerce_ignore_cases,
    "activity": coerce_ignore_cases,
}
