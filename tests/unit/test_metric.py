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

import socket

import pytest

from polytope_server.common.metric import Metric, MetricType, RequestStatusChange
from polytope_server.common.request import Status


class Test:
    def setup_method(self, method):
        pass

    def test_metric(self):
        m = Metric()
        assert m.uuid is not None
        assert m.timestamp is not None
        assert m.type is MetricType.GENERIC
        m2 = Metric()
        assert m.uuid != m2.uuid
        assert m.timestamp != m2.timestamp
        assert m.get_slots() == ["uuid", "timestamp", "type"]
        d = m.serialize()
        assert "uuid" in d
        assert "timestamp" in d
        assert "type" in d
        assert d["uuid"] == m.uuid
        m.deserialize(d)
        assert m.uuid == d["uuid"]
        assert m.timestamp == d["timestamp"]
        ts = m.timestamp
        m.update(uuid="new_id")
        assert m.uuid == "new_id"
        assert m.timestamp != ts
        assert m != m2
        m.deserialize(d)
        m2.deserialize(d)
        assert m == m2
        with pytest.raises(AttributeError):
            m.new_attr = "test"
        with pytest.raises(AttributeError):
            Metric(new_attr=0)

    def test_from_dict(self):
        d = {"uuid": "id001", "type": "request_status_change"}
        m = Metric(from_dict=d)
        assert m.uuid == "id001"
        assert m.type == MetricType.REQUEST_STATUS_CHANGE

    def test_sub_metric(self):
        m = RequestStatusChange(status=Status.PROCESSED, request_id="test-request-123", user_id="test-user")
        assert m.type == MetricType.REQUEST_STATUS_CHANGE
        assert "host" in m.get_slots()
        # host will be set automatically to socket.gethostname()
        assert m.host == socket.gethostname()
        assert "status" in m.get_slots()
        assert m.status == Status.PROCESSED
        assert "request_id" in m.get_slots()
        assert m.request_id == "test-request-123"
        assert "user_id" in m.get_slots()
        assert m.user_id == "test-user"
