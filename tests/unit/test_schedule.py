from datetime import date, time, timedelta

import pytest
from polytope_feature.utility.exceptions import PolytopeError

from polytope_server.common.schedule import (
    ScheduleReader,
    parse_mars_date,
    parse_mars_time,
    split_mars_param,
)


@pytest.fixture
def mock_schedule_file(tmp_path):
    # Create mock XML data
    xml_data = """
    <schedule>
        <product>
            <class>od</class>
            <stream>oper/wave</stream>
            <domain>g</domain>
            <time>00:00</time>
            <step>0000</step>
            <diss_type>an</diss_type>
            <release_time>05:35:00</release_time>
            <release_delta_day>1</release_delta_day>
        </product>
        <mars_only>
            <product>
                <class>od</class>
                <stream>enfo/waef</stream>
                <time>00:00</time>
                <release_time>06:40:00</release_time>
                <release_delta_day>1</release_delta_day>
            </product>
        </mars_only>
        <product>
            <class>ai</class>
            <stream>oper</stream>
            <domain>g</domain>
            <time>00:00</time>
            <step>0000</step>
            <diss_type>fc</diss_type>
            <release_time>08:34:00</release_time>
            <release_delta_day>0</release_delta_day>
        </product>
    </schedule>
    """
    schedule_file = tmp_path / "schedule.xml"
    schedule_file.write_text(xml_data)
    return schedule_file


def test_get_release_time_and_delta_day_match(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    release_time, delta_day = reader.get_release_time_and_delta_day(
        cclass="od", stream="oper/wave", domain="g", time_in="00:00", step="0000", ttype="an"
    )
    assert release_time == "05:35:00"
    assert delta_day == 1
    release_time, delta_day = reader.get_release_time_and_delta_day(
        cclass="od", stream="enfo", domain="g", time_in="00:00", step="0360", ttype="pf"
    )
    assert release_time == "06:40:00"
    assert delta_day == 1


def test_get_release_time_and_delta_day_no_match(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    release_time, delta_day = reader.get_release_time_and_delta_day(
        cclass="od", stream="nonexistent", domain="g", time_in="00:00", step="0000", ttype="an"
    )
    assert release_time is None
    assert delta_day is None


def test_check_released(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    date_in = "2023-10-01"
    cclass = "od"
    stream = "oper/wave"
    domain = "g"
    time_in = "00:00"
    step = "0000"
    diss_type = "an"
    reader.check_released(date_in, cclass, stream, domain, time_in, step, diss_type)

    stream = "enfo"
    step = "0100"
    reader.check_released(date_in, cclass, stream, domain, time_in, step, diss_type)

    with pytest.raises(Exception):
        reader.check_released(date_in, "od", "nonexistent", "g", "00:00", "0000", "an")


def test_split_mars_param():
    assert split_mars_param("0/1/2") == ["0", "1", "2"]
    assert split_mars_param("0/to/2") == "2"
    assert split_mars_param("0/to/7/by/2") == "7"


def test_parse_mars_date():
    assert parse_mars_date("2023-10-01") == date(2023, 10, 1)
    assert parse_mars_date("20231001") == date(2023, 10, 1)
    assert parse_mars_date("0") == date.today()
    assert parse_mars_date("-1") == date.today() - timedelta(days=1)
    pytest.raises(PolytopeError, parse_mars_date, "1")
    pytest.raises(PolytopeError, parse_mars_date, "2023274")
    pytest.raises(PolytopeError, parse_mars_date, "January")


def test_parse_mars_time():
    assert parse_mars_time("1230") == time(12, 30)
    assert parse_mars_time("0000") == time(0, 0)
    assert parse_mars_time("12:30") == time(12, 30)
    assert parse_mars_time("12") == time(12, 0)

    with pytest.raises(ValueError):
        parse_mars_time("invalid_time")

    with pytest.raises(ValueError):
        parse_mars_time("25:00")

    with pytest.raises(ValueError):
        parse_mars_time("123456")


polytope_requests = [
    # time_request =
    {
        "class": "od",
        "stream": "enfo",
        "type": "pf",
        "date": "-2",
        "time": "0000",
        "levtype": "sfc",
        "expver": "0001",
        "domain": "g",
        "param": "164/167/169",
        "number": "1",
        "step": "0/to/60",
        "feature": {
            "type": "timeseries",
            "points": [[-9.109280931080349, 38.78655345978706]],
            "axes": "step",
        },
    },
    # timeseries with step range
    {
        "class": "od",
        "stream": "enfo",
        "type": "pf",
        "date": "-2",
        "time": "0000",
        "levtype": "sfc",
        "expver": "0001",
        "domain": "g",
        "param": "164/167/169",
        "number": "1",
        "feature": {
            "type": "timeseries",
            "points": [[-9.109280931080349, 38.78655345978706]],
            "axes": "step",
            "range": {"start": 0, "end": 60, "step": 6},
        },
    },
    # trajectory with step points
    {
        "class": "od",
        "stream": "enfo",
        "type": "pf",
        "date": "-2",
        "time": "0000",
        "levtype": "sfc",
        "expver": "0001",
        "domain": "g",
        "param": "167",
        "number": [1],
        "feature": {
            "type": "trajectory",
            "points": [[0, 0, 1], [10, 10, 2], [20, 20, 3]],
            "inflation": [5, 5, 1],
            "inflate": "round",
            "axes": ["latitude", "longitude", "step"],
        },
    },
]


def test_polytope_schedule(mock_schedule_file):
    sr = ScheduleReader(str(mock_schedule_file))
    for r in polytope_requests:
        sr.check_released_polytope_request(r)


# needs to schedule.xml file included

# from polytope_server.common.schedule import SCHEDULE_READER

# def test_integration_test():
#     date_in = "20241113"
#     cclass = "od"
#     stream = "enfo"
#     domain = "g"
#     time_in = "00:00"
#     step = "0360"
#     diss_type = "cf"
#     SCHEDULE_READER.check_released(date_in, cclass, stream, domain, time_in, step, diss_type)
