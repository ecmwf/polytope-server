import pytest

from polytope_server.common.datasource.coercion import Coercion, CoercionError


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

    assert Coercion.coerce(request_mars) == request_out
    assert Coercion.coerce(request_json) == request_out


def test_date_coercion():

    from datetime import datetime, timedelta

    today = datetime.today()
    yyyymmdd = today.strftime("%Y%m%d")
    yyyy_mm_dd = today.strftime("%Y-%m-%d")
    yesterday = (today + timedelta(days=-1)).strftime("%Y%m%d")
    today = today.strftime("%Y%m%d")

    ok = [
        (20241114, "20241114"),
        ("20241114", "20241114"),
        ("2024-11-14", "20241114"),
        (int(yyyymmdd), yyyymmdd),
        (yyyymmdd, yyyymmdd),
        (yyyy_mm_dd, yyyymmdd),
        (-1, yesterday),
        (0, today),
    ]

    fail = [
        "2024-11-14T00:00:00",
        202401,
        2024010,
        1.0,
        [],
        {},
    ]

    for value, expected in ok:
        result = Coercion.coerce_date(value)
        assert result == expected

    for value in fail:
        with pytest.raises(CoercionError):
            Coercion.coerce_date(value)


def test_step_coercion():

    # Should accept integer or string, converted to string
    ok = [
        (2, "2"),
        ("1", "1"),
        (10, "10"),
        (0, "0"),
        ("0", "0"),
    ]

    fail = [-1, 1.0, [], {}]

    for value, expected in ok:
        result = Coercion.coerce_step(value)
        assert result == expected

    for value in fail:
        with pytest.raises(CoercionError):
            Coercion.coerce_step(value)


def test_number_coercion():

    # Should accept integer or string, converted to string
    ok = [(2, "2"), ("1", "1"), (10, "10")]

    fail = [-1, 0, 1.0, [], {}]

    for value, expected in ok:
        result = Coercion.coerce_number(value)
        assert result == expected

    for value in fail:
        with pytest.raises(CoercionError):
            Coercion.coerce_number(value)


def test_param_coercion():

    # OK, but should be converted
    ok = [
        (100, "100"),
        ("100", "100"),
        ("100.200", "100.200"),
        ("2t", "2t"),
    ]
    fail = [[], {}, 1.0]

    for value, expected in ok:
        result = Coercion.coerce_param(value)
        assert result == expected

    for value in fail:
        with pytest.raises(CoercionError):
            Coercion.coerce_param(value)


def test_time_coercion():

    # OK, but should be converted
    ok = [
        ("1200", "1200"),
        ("12", "1200"),
        ("1", "0100"),
        ("6", "0600"),
        ("12:00", "1200"),
        (0, "0000"),
        (12, "1200"),
        (1200, "1200"),
    ]
    fail = [
        "abc",
        25,
        2400,
        2401,
        -1,
        -10,
        [],
        {},
    ]

    for value, expected in ok:
        result = Coercion.coerce_time(value)
        assert result == expected

    for value in fail:
        with pytest.raises(CoercionError):
            Coercion.coerce_time(value)


def test_expver_coercion():
    expvers = [
        "0001",
        "001",
        "01",
        "1",
        1,
    ]

    for expver in expvers:
        result = Coercion.coerce_expver(expver)
        assert result == "0001"

    assert Coercion.coerce_expver("abcd") == "abcd"
    assert Coercion.coerce_expver(10) == "0010"
    assert Coercion.coerce_expver("1abc") == "1abc"

    with pytest.raises(CoercionError):
        Coercion.coerce_expver("abcde")  # too long

    with pytest.raises(CoercionError):
        Coercion.coerce_expver("abc")  # too short

    with pytest.raises(CoercionError):
        Coercion.coerce_expver(1.0)  # float

    with pytest.raises(CoercionError):
        Coercion.coerce_expver([])

    with pytest.raises(CoercionError):
        Coercion.coerce_expver({})

    with pytest.raises(CoercionError):
        Coercion.coerce_expver(["a", "b", "c", "d"])
