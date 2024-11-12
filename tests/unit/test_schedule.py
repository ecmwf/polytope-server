import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import pytest

from polytope_server.common.schedule import ScheduleReader, find_tag


@pytest.fixture
def mock_schedule_file(tmp_path):
    # Create mock XML data
    xml_data = """
    <root>
        <product>
            <class>od</class>
            <stream>oper</stream>
            <domain>g</domain>
            <time>00:00</time>
            <diss_type>fc</diss_type>
            <step>0</step>
            <release_time>12:00:00</release_time>
            <release_delta_day>1</release_delta_day>
        </product>
        <product>
            <class>ai</class>
            <stream>scda</stream>
            <domain>m</domain>
            <time>06:00</time>
            <diss_type>an</diss_type>
            <step>1</step>
            <release_time>18:00:00</release_time>
            <release_delta_day>2</release_delta_day>
        </product>
    </root>
    """
    schedule_file = tmp_path / "schedule.xml"
    schedule_file.write_text(xml_data)
    return schedule_file


def test_get_release_time_and_delta_day_match(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    release_time, delta_day = reader.get_release_time_and_delta_day(
        cclass="od", stream="oper", domain="g", time="00:00", step="0", diss_type="fc"
    )
    assert release_time == "12:00:00"
    assert delta_day == 1


def test_get_release_time_and_delta_day_no_match(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    release_time, delta_day = reader.get_release_time_and_delta_day(
        cclass="od", stream="nonexistent", domain="g", time="00:00", step="0", diss_type="fc"
    )
    assert release_time is None
    assert delta_day is None


def test_is_released(mock_schedule_file):
    reader = ScheduleReader(str(mock_schedule_file))
    date_in = datetime.now() - timedelta(days=2)
    assert reader.is_released(date_in, "od", "oper", "g", "00:00", "0", "fc") is True

    date_in = datetime.now() + timedelta(days=2)
    assert reader.is_released(date_in, "od", "oper", "g", "00:00", "0", "fc") is False


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


def test_xml_read():
    sr = ScheduleReader("schedule.xml")
    len(sr.products) == 500
