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

import pytest

import polytope_server.common.logging as logging
from polytope_server.common.config import ConfigParser


def pytest_addoption(parser):

    parser.addoption("--config")


def pytest_configure(config):

    # reading in configuration
    pytest.polytope_config = ConfigParser().read()

    polytope_config_auth = copy.deepcopy(pytest.polytope_config)

    pytest.polytope_config_auth = polytope_config_auth

    logging.setup(pytest.polytope_config, source_name="polytope_server.tests.internal")

    # setting markers
    def build_labels(config_dict, prefix=None, leafs_only=False):
        labels = []
        if prefix:
            prefix = [prefix]
        else:
            prefix = []
        if isinstance(config_dict, dict):
            for k, v in config_dict.items():
                label = "_".join(prefix + [k.replace("-", "_")])
                if not leafs_only:
                    labels.append(label)
                labels += build_labels(v, label, leafs_only)
        elif isinstance(config_dict, list):
            for v in config_dict:
                label = "_".join(prefix + ["any"])
                labels += build_labels(v, label, leafs_only)
        else:
            if not config_dict:
                if leafs_only:
                    labels += prefix
            else:
                if str(config_dict).isalnum():
                    labels.append("_".join(prefix + [str(config_dict)]))
        return labels

    markers = {
        "frontend": None,
        "request_store": {"mongodb": None},
        "caching": {"mongodb": None, "redis": None},
        "queue": {"rabbitmq": None},
        "worker": None,
        "broker": None,
        "testrunner": None,
        "staging": {"s3": None, "polytope": None},
        "authentication": [
            {"type": "ecmwfapi"},
            {"type": "plain"},
        ],
        "authorization": [
            {"type": "ldap"},
            {"type": "plain"},
        ],
    }

    markers = build_labels(markers, leafs_only=True)
    markers.append("basic")

    labels = build_labels(pytest.polytope_config)
    labels.append("basic")
    labels = list(set.intersection(set(markers), set(labels)))

    for marker in markers:
        config.addinivalue_line("markers", marker)

    pytest.labels = labels
    pytest.markers = markers


def pytest_runtest_setup(item):
    for marker in [mark.name for mark in item.iter_markers()]:
        if marker in pytest.markers:
            if marker not in pytest.labels:
                pytest.skip(
                    "This test has been skipped because the "
                    + "component it tests has not been deployed as per "
                    + "the provided Polytope configuration."
                )
