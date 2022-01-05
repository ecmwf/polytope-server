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


def pytest_addoption(parser):
    parser.addoption("--internal_address", action="append")
    parser.addoption("--internal_port", action="append")
    parser.addoption("--address", action="append")
    parser.addoption("--port", action="append")
    parser.addoption("--admin_username", action="append")
    parser.addoption("--admin_password", action="append")
    parser.addoption("--user_email", action="append")
    parser.addoption("--user_key", action="append")


def pytest_generate_tests(metafunc):
    params = [
        "internal_address",
        "internal_port",
        "address",
        "port",
        "admin_username",
        "admin_password",
        "user_email",
        "user_key",
    ]
    for param in params:
        if param in metafunc.fixturenames:
            val = metafunc.config.getoption(param)
            if val:
                metafunc.parametrize(param, val)
            else:
                raise Exception("MISSING PARAM --%s" % param)


class ValueStorage:
    file_config = None
    address = None
    port = None
    admin_username = None
    admin_password = None
    user_email = None
    user_key = None
