"""
End-to-end tests: polytope-client (v1) against a live deployment.

    POLYTOPE_E2E_ADDRESS=https://polytope-822ff553.ecmwf.int  pytest -m e2e
"""

import os

import pytest

E2E_ADDRESS = os.environ.get(
    "POLYTOPE_E2E_ADDRESS", "https://polytope-822ff553.ecmwf.int"
)

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session")
def client(tmp_path_factory):
    from polytope.api import Client

    config_dir = tmp_path_factory.mktemp("polytope-e2e-config")
    return Client(
        address=E2E_ADDRESS,
        user_key=os.environ.get("POLYTOPE_USER_KEY", "e2e-test-key"),
        user_email=os.environ.get("POLYTOPE_USER_EMAIL", "e2e@test.invalid"),
        quiet=True,
        config_path=str(config_dir),
    )


SAMPLE_REQUEST = {
    "class": "od",
    "stream": "oper",
    "type": "fc",
    "expver": "1",
    "levtype": "sfc",
    "param": "2t",
    "date": "20250101",
    "time": "0000",
    "step": "0",
}


def test_ping(client):
    client.ping()


def test_list_collections(client):
    collections = client.list_collections()
    assert isinstance(collections, list)
    assert len(collections) > 0


def test_retrieve(client, tmp_path):
    output = str(tmp_path / "result.grib")
    client.retrieve("all", SAMPLE_REQUEST, output_file=output)
    assert os.path.getsize(output) > 0


time curl -X POST https://polytope-822ff553.ecmwf.int/api/v2/requests -H "Content-Type: application/json" -d '{"class":"od","stream":"enfo","type":"pf","date":20260318,"time":"0000","levtype":"sfc","expver":"0001","domain":"g","param":"164","number":"1","feature":{"type":"timeseries","points":[[38.9,-9.1]],"time_axis":"step","axes":["latitude","longitude"],"range":{"start":0,"end":3}}}'
time curl -X POST https://polytope-822ff553.ecmwf.int/api/v2/requests -H "Content-Type: application/json" -d '{"class":"od","stream":"enfo","type":"pf","date":-1,"time":"0000","levtype":"sfc","expver":"0001","domain":"g","param":"164","number":"1","feature":{"type":"timeseries","points":[[38.9,-9.1]],"time_axis":"step","axes":["latitude","longitude"],"range":{"start":0,"end":3}}}'
time curl -X POST https://polytope-822ff553.ecmwf.int/api/v2/requests -H "Content-Type: application/json" -d '{"class":"od","stream":"enfo","type":"pf","date":20260318,"time":"0000","levtype":"sfc","expver":"0001","domain":"g","param":"164","number":"1"}'

time curl -L "https://polytope-822ff553.ecmwf.int/edr/collections/operational-data/position?f=json&datetime=2026-03-18T00%3A00%3A00Z%2F2026-03-20T00%3A00%3A00Z&parameter-name=2t&coords=POINT(60+13)"