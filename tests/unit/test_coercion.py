from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from polytope_server.common.coercion import (
    CoercionError,
    coerce,
    coerce_date,
    coerce_expver,
    coerce_number,
    coerce_param,
    coerce_step,
    coerce_time,
)


def test_coerce():

    # mars-like
    request_mars = {
        "class": "od",
        "stream": "enfo",
        "type": "pf",
        "date": "2024-11-14",
        "time": 12,
        "levtype": "sfc",
        "expver": 1,
        "domain": "g",
        "param": "164/166/167/169",
        "number": "1/2",
        "step": "0/to/360/by/6",
        "feature": {  # dict ignored
            "foo": "bar",
        },
    }

    # json-like
    request_json = {
        "class": "od",
        "stream": ["enfo"],
        "type": "pf",
        "date": "2024-11-14",
        "time": 12,
        "levtype": "sfc",
        "expver": [1],
        "domain": "g",
        "param": [164, 166, 167, "169"],
        "number": "1/2",
        "step": "0/to/360/by/6",
        "feature": {  # dict ignored
            "foo": "bar",
        },
    }

    request_out = {
        "class": "od",
        "stream": "enfo",
        "type": "pf",
        "date": "20241114",
        "time": "1200",
        "levtype": "sfc",
        "expver": "0001",
        "domain": "g",
        "param": "164/166/167/169",
        "number": "1/2",
        "step": "0/to/360/by/6",
        "feature": {  # dict ignored
            "foo": "bar",
        },
    }
    for r in [request_json, request_mars]:
        r = coerce(r)
        for key in r:
            if isinstance(r[key], list):
                r[key] = "/".join(r[key])
        assert r == request_out


@pytest.mark.parametrize(
    "value, expected",
    [
        (20241114, "20241114"),
        ("20241114", "20241114"),
        ("2024-11-14", "20241114"),
        (-1, (datetime.today() + timedelta(days=-1)).strftime("%Y%m%d")),
        (0, datetime.today().strftime("%Y%m%d")),
        ("-1", (datetime.today() + timedelta(days=-1)).strftime("%Y%m%d")),
        ("0", datetime.today().strftime("%Y%m%d")),
    ],
)
def test_date_coercion_ok(value, expected):
    result = coerce_date(value)
    assert result == expected


@pytest.mark.parametrize(
    "value",
    [
        "2024-11-14T00:00:00",
        202401,
        2024010,
        1.0,
        [],
        {},
    ],
)
def test_date_coercion_fail(value):
    with pytest.raises(CoercionError):
        coerce_date(value)


@pytest.mark.parametrize(
    "value, expected",
    [
        (2, "2"),
        ("1", "1"),
        (10, "10"),
        (0, "0"),
        ("0", "0"),
        ("70m", "70m"),
        ("1h15m", "1h15m"),
        ("2h", "2h"),
        ("1-2", "1-2"),
        ("1h-3h", "1h-3h"),
        ("1h30m-32", "1h30m-32"),
        ("1m30s-3m", "1m30s-3m"),
        ("3d", "3d"),
    ],
)
def test_step_coercion_ok(value, expected):
    result = coerce_step(value)
    assert result == expected


@pytest.mark.parametrize(
    "value",
    [
        -1,
        1.0,
        [],
        {},
        "1h-3s30m",
        "3m20d",
    ],
)
def test_step_coercion_fail(value):
    with pytest.raises(CoercionError):
        coerce_step(value)


@pytest.mark.parametrize("value, expected", [(2, "2"), ("1", "1"), (10, "10")])
def test_number_coercion_ok(value, expected):
    result = coerce_number(value)
    assert result == expected


@pytest.mark.parametrize("value", [-1, 0, 1.0, [], {}])
def test_number_coercion_fail(value):
    with pytest.raises(CoercionError):
        coerce_number(value)


@patch("polytope_server.common.coercion.get_config", lambda: {"number_allow_zero": True})
def test_number_coercion_allow_zero():
    assert coerce_number(0) == "0"


@pytest.mark.parametrize(
    "value, expected",
    [
        (100, "100"),
        ("100", "100"),
        ("100.200", "100.200"),
        ("2t", "2t"),
    ],
)
def test_param_coercion_ok(value, expected):
    result = coerce_param(value)
    assert result == expected


@pytest.mark.parametrize("value", [[], {}, 1.0])
def test_param_coercion_fail(value):
    with pytest.raises(CoercionError):
        coerce_param(value)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1200", "1200"),
        ("12", "1200"),
        ("1", "0100"),
        ("6", "0600"),
        ("12:00", "1200"),
        (0, "0000"),
        (12, "1200"),
        (1200, "1200"),
    ],
)
def test_time_coercion_ok(value, expected):
    result = coerce_time(value)
    assert result == expected


@pytest.mark.parametrize(
    "value",
    [
        "abc",
        25,
        2400,
        2401,
        -1,
        -10,
        [],
        {},
    ],
)
def test_time_coercion_fail(value):
    with pytest.raises(CoercionError):
        coerce_time(value)


@pytest.mark.parametrize("value", ["0001", "001", "01", "1", 1])
def test_expver_coercion_ok_padded(value):
    result = coerce_expver(value)
    assert result == "0001"


@pytest.mark.parametrize(
    "value, expected",
    [
        ("abcd", "abcd"),
        (10, "0010"),
        ("1abc", "1abc"),
    ],
)
def test_expver_coercion_ok_passthrough(value, expected):
    assert coerce_expver(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "abcde",
        "abc",
        1.0,
        [],
        {},
        ["a", "b", "c", "d"],
    ],
)
def test_expver_coercion_fail(value):
    with pytest.raises(CoercionError):
        coerce_expver(value)
