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

import copy
import logging
from unittest.mock import MagicMock, patch

import yaml

import polytope_server.common.collection as collection
import polytope_server.common.config as polytope_config
from polytope_server.common.request import PolytopeRequest


class Test:
    def setup_method(self, method):

        self.config = {
            "datasources": {"a_datasource": {"type": "echo"}},
            "authentication": {"an_authentication": {"type": "none"}},
            "authorization": {"an_authorization": {"type": "none"}},
            "collections": {
                "a_collection": {
                    "authentication": "an_authentication",
                    "authorization": "an_authorization",
                    "roles": ["role1", "role2"],
                    "datasources": [{"name": "a_datasource", "match": "some_config"}],
                }
            },
        }
        polytope_config.global_config = self.config

    def test_collection(self):
        collection.create_collections(self.config["collections"])

    def _make_request(self, coerced_request=None):
        """Build a minimal PolytopeRequest suitable for dispatch tests."""
        req = PolytopeRequest()
        req.user_request = yaml.dump({"class": "od", "expver": "1"})
        req.coerced_request = coerced_request if coerced_request is not None else {}
        req.user = MagicMock()
        req.user_message = ""
        req.id = "test-id-001"
        return req

    def _make_collection(self):
        cols = collection.create_collections(self.config["collections"])
        return cols["a_collection"]

    # ------------------------------------------------------------------
    # (a) + (b): legacy request (coerced_request == {}) triggers fallback
    #            and a warning is logged
    # ------------------------------------------------------------------
    def test_legacy_request_fallback_coerces_and_warns(self, caplog):
        """When coerced_request is falsy, dispatch coerces at call time and warns."""
        col = self._make_collection()
        req = self._make_request(coerced_request={})  # legacy: empty dict

        coerced_value = {"class": "od", "expver": "1"}

        with patch(
            "polytope_server.common.collection.coercion.coerce", return_value=coerced_value
        ) as mock_coerce, patch("polytope_server.common.collection.DataSource.match", return_value="success"), patch(
            "polytope_server.common.collection.create_datasource"
        ) as mock_create_ds:
            mock_ds = MagicMock()
            mock_create_ds.return_value = mock_ds

            with caplog.at_level(logging.WARNING, logger="polytope_server.common.collection"):
                col.dispatch(req, None)

        # (a) coerce was called with the parsed user_request
        mock_coerce.assert_called_once_with(yaml.safe_load(req.user_request))

        # (b) a warning was emitted
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warning_records, "Expected a WARNING log during legacy fallback"
        assert any("coerced_request" in r.message or "Legacy" in r.message for r in warning_records)

    # ------------------------------------------------------------------
    # (c): coerced_request is populated on the request object after dispatch
    # ------------------------------------------------------------------
    def test_legacy_request_coerced_request_populated_after_dispatch(self):
        """After dispatch, request.coerced_request must be set to the coerced value."""
        col = self._make_collection()
        req = self._make_request(coerced_request={})

        coerced_value = {"class": "od", "expver": "1"}

        with patch("polytope_server.common.collection.coercion.coerce", return_value=coerced_value), patch(
            "polytope_server.common.collection.DataSource.match", return_value="success"
        ), patch("polytope_server.common.collection.create_datasource") as mock_create_ds:
            mock_ds = MagicMock()
            mock_create_ds.return_value = mock_ds

            col.dispatch(req, None)

        assert req.coerced_request == coerced_value

    # ------------------------------------------------------------------
    # (d): matching operates on a deep copy — persisted coerced_request
    #      is not mutated by datasource matching
    # ------------------------------------------------------------------
    def test_dispatch_does_not_mutate_coerced_request(self):
        """DataSource.match receives a deep copy; the stored coerced_request is unchanged."""
        col = self._make_collection()
        coerced_value = {"class": "od", "expver": "1", "param": ["130", "131"]}
        req = self._make_request(coerced_request=copy.deepcopy(coerced_value))

        received_by_match = {}

        def mutating_match(ds_config, coerced_ur, user):
            # Simulate a datasource that mutates the dict it receives
            received_by_match["ref"] = coerced_ur
            coerced_ur["param"] = ["MUTATED"]
            coerced_ur["extra_key"] = "injected"
            return "success"

        with patch("polytope_server.common.collection.DataSource.match", side_effect=mutating_match), patch(
            "polytope_server.common.collection.create_datasource"
        ) as mock_create_ds:
            mock_ds = MagicMock()
            mock_create_ds.return_value = mock_ds

            col.dispatch(req, None)

        # The copy passed to match was mutated …
        assert received_by_match["ref"]["param"] == ["MUTATED"]
        # … but the persisted coerced_request is intact
        assert req.coerced_request == coerced_value, f"coerced_request was mutated: {req.coerced_request}"
