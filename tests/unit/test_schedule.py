import xml.etree.ElementTree as ET
from datetime import date, timedelta

import pytest
from polytope_feature.utility.exceptions import PolytopeError

from polytope_server.common.schedule import (
    ScheduleReader,
    find_tag,
    parse_mars_date,
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
        cclass="od", stream="oper/wave", domain="g", time="00:00", step="0000", diss_type="an"
    )
    assert release_time == "05:35:00"
    assert delta_day == 1


def test_get_release_time_and_delta_day_no_match(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    release_time, delta_day = reader.get_release_time_and_delta_day(
        cclass="od", stream="nonexistent", domain="g", time="00:00", step="0000", diss_type="an"
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

    with pytest.raises(Exception):
        reader.check_released(date_in, "od", "nonexistent", "g", "00:00", "0000", "an")


def test_find_tag():
    xml_data = """
    <root>
        <product>
            <diss_domain>g</diss_domain>
            <domain>m</domain>
        </product>
    </root>
    """
    tree = ET.ElementTree(ET.fromstring(xml_data))
    product = tree.find("product")
    assert find_tag(product, "domain") == "g"

    xml_data = """
    <root>
        <product>
            <domain>m</domain>
        </product>
    </root>
    """
    tree = ET.ElementTree(ET.fromstring(xml_data))
    product = tree.find("product")
    assert find_tag(product, "domain") == "m"

    xml_data = """
    <root>
        <product>
        </product>
    </root>
    """
    tree = ET.ElementTree(ET.fromstring(xml_data))
    product = tree.find("product")
    with pytest.raises(IOError):
        find_tag(product, "domain")


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
