#
# Copyright 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

from datetime import date, datetime, timedelta

import pytest

from polytope_server.common.datasource.date_check import (
    DateError,
    date_check,
    date_in_mars_rule,
    expand_mars_dates,
    parse_mars_date_token,
)


def d(offset):
    """Return the date object for today + offset days."""
    return (datetime.today() + timedelta(days=offset)).date()


def ds(offset):
    """Return YYYYMMDD string for today + offset days."""
    return (datetime.today() + timedelta(days=offset)).strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# parse_mars_date_token
# ---------------------------------------------------------------------------


class TestParseMarsDateToken:
    def test_relative_negative_one(self):
        assert parse_mars_date_token("-1").date() == d(-1)

    def test_relative_negative_large(self):
        assert parse_mars_date_token("-365").date() == d(-365)

    def test_relative_zero(self):
        assert parse_mars_date_token("0").date() == d(0)

    def test_absolute_yyyymmdd(self):
        assert parse_mars_date_token("20250125").date() == date(2025, 1, 25)

    def test_absolute_iso(self):
        assert parse_mars_date_token("2023-04-23").date() == date(2023, 4, 23)

    def test_absolute_iso_leap_day(self):
        assert parse_mars_date_token("2024-02-29").date() == date(2024, 2, 29)

    def test_invalid_string(self):
        with pytest.raises(DateError):
            parse_mars_date_token("notadate")

    def test_empty_string(self):
        with pytest.raises(DateError):
            parse_mars_date_token("")

    # Edge case: positive integer — NOT treated as relative (no leading 0 or -)
    # "1" is an invalid YYYYMMDD and not a relative token, so it should fail.
    def test_positive_integer_not_supported_as_relative(self):
        with pytest.raises(DateError):
            parse_mars_date_token("1")


# ---------------------------------------------------------------------------
# expand_mars_dates
# ---------------------------------------------------------------------------


class TestExpandMarsDates:
    # Single dates
    def test_single_relative(self):
        assert expand_mars_dates("-1") == [d(-1)]
        assert expand_mars_dates("0") == [d(0)]

    def test_single_absolute(self):
        assert expand_mars_dates("20250125") == [date(2025, 1, 25)]
        assert expand_mars_dates("2023-04-23") == [date(2023, 4, 23)]

    # Lists
    def test_list_relative(self):
        assert expand_mars_dates("-1/-5/-10") == [d(-1), d(-5), d(-10)]

    def test_list_mixed_formats(self):
        # from the spec: "20250125/-5/2023-04-23"
        result = expand_mars_dates("20250125/-5/2023-04-23")
        assert result == [date(2025, 1, 25), d(-5), date(2023, 4, 23)]

    def test_list_two_elements(self):
        # Two-element list must NOT be mistaken for a range
        result = expand_mars_dates("-1/-20")
        assert result == [d(-1), d(-20)]
        assert len(result) == 2

    # Ranges
    def test_range(self):
        # Ascending range
        result = expand_mars_dates("2025-01-01/to/2025-01-05")
        assert result == [date(2025, 1, i) for i in range(1, 6)]
        # Descending range is valid: start > end in calendar terms
        result = expand_mars_dates("2025-01-05/to/2025-01-01")
        assert result == [date(2025, 1, i) for i in range(5, 0, -1)]
        # Relative descending: -1/to/-20
        result = expand_mars_dates("-1/to/-20")
        assert result[0] == d(-1)
        assert result[-1] == d(-20)
        assert len(result) == 20

    # Ranges with step
    def test_stepped_range(self):
        # Relative: -4/to/-20/by/4 -> -4, -8, -12, -16, -20
        result = expand_mars_dates("-4/to/-20/by/4")
        assert result == [d(-4), d(-8), d(-12), d(-16), d(-20)]
        # Absolute: 2024-02-21/to/2025-03-01/by/10
        result = expand_mars_dates("2024-02-21/to/2025-03-01/by/10")
        assert result[0] == date(2024, 2, 21)
        assert all((r - date(2024, 2, 21)).days % 10 == 0 for r in result)
        # Symmetric: forward and backward should produce same set
        fwd = set(expand_mars_dates("-4/to/-20/by/4"))
        rev = set(expand_mars_dates("-20/to/-4/by/4"))
        assert fwd == rev
        # Uneven step: stops before reaching exact end
        result = expand_mars_dates("-4/to/-21/by/4")
        assert d(-20) in result
        assert d(-21) not in result

    def test_case_insensitive_to(self):
        assert expand_mars_dates("-1/TO/-3") == expand_mars_dates("-1/to/-3")

    def test_case_insensitive_by(self):
        assert expand_mars_dates("-4/to/-20/BY/4") == expand_mars_dates("-4/to/-20/by/4")


# ---------------------------------------------------------------------------
# date_in_mars_rule
# ---------------------------------------------------------------------------


class TestDateInMarsRule:
    # Single date rules
    def test_single_date(self):
        assert date_in_mars_rule(d(-1), "-1") is True
        assert date_in_mars_rule(d(-2), "-1") is False
        assert date_in_mars_rule(date(2025, 1, 25), "20250125") is True

    # List rules
    def test_list(self):
        assert date_in_mars_rule(d(-1), "-1/-5/-10") is True
        assert date_in_mars_rule(d(-5), "-1/-5/-10") is True
        assert date_in_mars_rule(d(-10), "-1/-5/-10") is True
        assert date_in_mars_rule(d(-3), "-1/-5/-10") is False

    def test_list_mixed_formats(self):
        rule = "20250125/-5/2023-04-23"
        assert date_in_mars_rule(date(2025, 1, 25), rule) is True
        assert date_in_mars_rule(d(-5), rule) is True
        assert date_in_mars_rule(date(2023, 4, 23), rule) is True
        assert date_in_mars_rule(date(2025, 1, 26), rule) is False

    # Range rules
    def test_range(self):
        assert date_in_mars_rule(d(-1), "-1/to/-20") is True
        assert date_in_mars_rule(d(-10), "-1/to/-20") is True
        assert date_in_mars_rule(d(-20), "-1/to/-20") is True
        assert date_in_mars_rule(d(0), "-1/to/-20") is False
        assert date_in_mars_rule(d(-21), "-1/to/-20") is False
        # Inverted range covers same dates
        assert date_in_mars_rule(d(-10), "-20/to/-1") is True

    # Stepped range rules
    def test_stepped_range(self):
        # On step: -4, -8, -12, -16, -20
        assert date_in_mars_rule(d(-4), "-4/to/-20/by/4") is True
        assert date_in_mars_rule(d(-8), "-4/to/-20/by/4") is True
        assert date_in_mars_rule(d(-20), "-4/to/-20/by/4") is True
        # Off step or outside range
        assert date_in_mars_rule(d(-5), "-4/to/-20/by/4") is False
        assert date_in_mars_rule(d(-3), "-4/to/-20/by/4") is False

    def test_stepped_range_absolute(self):
        # 2024-02-21/to/2025-03-01/by/10
        start = date(2024, 2, 21)
        rule = "2024-02-21/to/2025-03-01/by/10"
        assert date_in_mars_rule(start, rule) is True
        assert date_in_mars_rule(start + timedelta(days=10), rule) is True
        assert date_in_mars_rule(start + timedelta(days=11), rule) is False


# ---------------------------------------------------------------------------
# date_check — from the user's examples
# ---------------------------------------------------------------------------


class TestDateCheckNewStyle:
    def test_single_relative(self):
        assert date_check(ds(-1), ["-1"]) is True
        with pytest.raises(DateError):
            date_check(ds(-2), ["-1"])

    def test_list_rule(self):
        assert date_check(ds(-1), ["-1/-5/-10"]) is True
        assert date_check(ds(-5), ["-1/-5/-10"]) is True
        with pytest.raises(DateError):
            date_check(ds(-3), ["-1/-5/-10"])

    def test_range_rule(self):
        assert date_check(ds(-1), ["-1/to/-20"]) is True
        assert date_check(ds(-10), ["-1/to/-20"]) is True
        with pytest.raises(DateError):
            date_check(ds(-21), ["-1/to/-20"])

    def test_stepped_range_rule(self):
        assert date_check(ds(-4), ["-4/to/-20/by/4"]) is True
        assert date_check(ds(-8), ["-4/to/-20/by/4"]) is True
        with pytest.raises(DateError):
            date_check(ds(-5), ["-4/to/-20/by/4"])

    def test_mixed_formats(self):
        rule = ["20250125/-5/2023-04-23"]
        assert date_check("20250125", rule) is True
        assert date_check(ds(-5), rule) is True
        with pytest.raises(DateError):
            date_check("20250126", rule)

    # --- Multiple rules: OR logic ---

    def test_or_logic(self):
        # Matches first rule
        assert date_check(ds(-1), ["-1/-5/-10", "-20/to/-30"]) is True
        # Matches second rule
        assert date_check(ds(-25), ["-1/-5/-10", "-20/to/-30"]) is True
        # Matches no rule
        with pytest.raises(DateError):
            date_check(ds(-15), ["-1/-5/-10", "-20/to/-30"])

    # --- User date as Mars string (range/list) ---

    def test_user_date_range(self):
        # All within rule
        assert date_check(f"{ds(-5)}/to/{ds(-10)}", ["-1/to/-20"]) is True
        # Partially outside rule
        with pytest.raises(DateError):
            date_check(f"{ds(-1)}/to/{ds(-25)}", ["-1/to/-20"])

    def test_user_date_list(self):
        # All match
        assert date_check(f"{ds(-1)}/{ds(-5)}/{ds(-10)}", ["-1/to/-20"]) is True
        # One outside
        with pytest.raises(DateError):
            date_check(f"{ds(-1)}/{ds(-5)}/{ds(-25)}", ["-1/to/-20"])

    def test_user_date_stepped_range(self):
        # Exact match
        assert date_check(f"{ds(-4)}/to/{ds(-20)}/by/4", ["-4/to/-20/by/4"]) is True
        # Subset of allowed range
        assert date_check(f"{ds(-4)}/to/{ds(-20)}/by/4", ["-1/to/-20"]) is True
        # Exceeds rule
        with pytest.raises(DateError):
            date_check(f"{ds(-4)}/to/{ds(-24)}/by/4", ["-1/to/-20"])

    def test_empty_allowed_values(self):
        assert date_check(ds(-1), []) is True


# ---------------------------------------------------------------------------
# date_check — old-style rules: backward compatibility
# ---------------------------------------------------------------------------


class TestDateCheckOldStyle:
    def test_single(self):
        assert date_check(ds(-32), [">30d"]) is True
        with pytest.raises(DateError):
            date_check(ds(-5), [">30d"])

    def test_multiple_and_logic(self):
        # Both conditions must be satisfied
        assert date_check(ds(-32), [">30d", "<40d"]) is True
        with pytest.raises(DateError):
            date_check(ds(-32), [">30d", "<20d"])

    def test_range(self):
        # All dates in range must satisfy rule
        assert date_check(f"{ds(-60)}/to/{ds(-40)}", [">30d"]) is True
        with pytest.raises(DateError):
            date_check(f"{ds(-60)}/to/{ds(-25)}", [">30d"])
