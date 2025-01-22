import copy
from datetime import datetime, timedelta
from typing import Any, Dict
import re


class CoercionError(Exception):
    pass


class Coercion:

    allow_ranges = ["number", "step", "date", "time"]
    allow_lists = ["class", "stream", "type", "expver", "param", "number", "date", "step", "time"]

    @staticmethod
    def coerce(request: Dict[str, Any]) -> Dict[str, Any]:
        request = copy.deepcopy(request)
        for key, value in request.items():
            request[key] = Coercion.coerce_value(key, value)
        return request

    @staticmethod
    def coerce_value(key: str, value: Any):
        if key in Coercion.coercer:
            coercer_func = Coercion.coercer[key]

            if isinstance(value, list):
                # Coerce each item in the list
                coerced_values = [Coercion.coerce_value(key, v) for v in value]
                return coerced_values
            elif isinstance(value, str):

                if "/to/" in value and key in Coercion.allow_ranges:
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
                elif "/" in value and key in Coercion.allow_lists:
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

    @staticmethod
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

    @staticmethod
    def coerce_step(value: Any) -> str:

        if isinstance(value, int):
            if value < 0:
                raise CoercionError("Step must be greater than or equal to 0.")
            else:
                return str(value)
        elif isinstance(value, str):
            try:
                if int(value) < 0:
                    raise CoercionError("Step must be greater than or equal to 0.")
                else:
                    return value
            except ValueError:
                # value cannot be converted to a digit, but we would like to match step ranges too
                pattern = r"^\d+-\d+$"
                if re.match(pattern, value):
                    return value
                else:
                    raise CoercionError("Invalid type, expected integer step or step range.")
        else:
            raise CoercionError("Invalid type, expected integer or string.")

    @staticmethod
    def coerce_number(value: Any) -> str:

        if isinstance(value, int):
            if value <= 0:
                raise CoercionError("Number must be a positive value.")
            else:
                return str(value)
        elif isinstance(value, str):
            if not value.isdigit() or int(value) <= 0:
                raise CoercionError("Number must be a positive integer.")
            return value
        else:
            raise CoercionError("Invalid type, expected integer or string.")

    @staticmethod
    def coerce_param(value: Any) -> str:
        if isinstance(value, int):
            return str(value)
        elif isinstance(value, str):
            return value
        else:
            raise CoercionError("Invalid param type, expected integer or string.")

    @staticmethod
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

        # # Format time as HHMM
        # time_str = f"{hour:02d}{minute:02d}"
        # return time_str

        # Validate hour and minute
        if not (0 <= hour <= 23):
            raise CoercionError("Hour must be between 0 and 23.")
        if not (0 <= minute <= 59):
            raise CoercionError("Minute must be between 0 and 59.")
        if minute != 0:
            # In your test cases, minute must be zero
            raise CoercionError("Minute must be zero.")

        # Format time as HHMM
        time_str = f"{hour:02d}{minute:02d}"
        return time_str

    @staticmethod
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

    coercer = {
        "date": coerce_date,
        "step": coerce_step,
        "number": coerce_number,
        "param": coerce_param,
        "time": coerce_time,
        "expver": coerce_expver,
    }
