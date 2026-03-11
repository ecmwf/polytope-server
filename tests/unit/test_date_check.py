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
    def test_stepped_range_in_rule_raises(self):
        with pytest.raises(DateError):
            date_in_mars_rule(d(-4), "-4/to/-20/by/4")


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
        # Subset of allowed range: only boundaries are checked
        assert date_check(f"{ds(-4)}/to/{ds(-20)}/by/4", ["-1/to/-20"]) is True
        # Exceeds rule
        with pytest.raises(DateError):
            date_check(f"{ds(-4)}/to/{ds(-24)}/by/4", ["-1/to/-20"])

    def test_user_date_range_or_logic(self):
        # Matches first rule
        assert date_check(f"{ds(-5)}/to/{ds(-10)}", ["-1/to/-20", "2024-01-01/to/2024-12-31"]) is True
        # Matches second rule
        assert date_check("2024-01-01/to/2024-08-31", ["-1/to/-20", "2024-01-01/to/2024-12-31"]) is True
        # Start matches one rule, end another
        with pytest.raises(DateError):
            date_check(f"{ds(-5)}/to/2024-08-31", ["-1/to/-20", "2024-01-01/to/2024-12-31"])
        # Matches no rule
        with pytest.raises(DateError):
            date_check(f"{ds(-5)}/to/{ds(30)}", ["-1/to/-20", "2024-01-01/to/2024-12-31"])

        # start matches one rule, but end matches another
        with pytest.raises(DateError):
            date_check(f"{ds(-5)}/to/{ds(30)}", ["-1/to/-20", "-30"])

        date_check("-5/-30", ["-1/to/-20", "-30"]) is True

    def test_empty_allowed_values(self):
        assert date_check(ds(-1), []) is True


# ---------------------------------------------------------------------------
# date_check — comparative rules: backward compatibility
# ---------------------------------------------------------------------------


class TestDateCheckComparative:
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

    def test_edges(self):
        # Edge cases: exactly 30 days ago should NOT satisfy ">30d"
        with pytest.raises(DateError):
            date_check(ds(-29), [">30d"])
        # Exactly 31 days ago should satisfy ">30d"
        assert date_check(ds(-30), [">30d"]) is True
