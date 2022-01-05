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

import pytest

import polytope_server.common.config as polytope_config


class Test:
    def setup_method(self, method):

        parser = polytope_config.ConfigParser()
        self.config = parser.read(pytest.basic_config)
        self.unused = parser.unused_cli_args()

    def teardown_method(self, method):
        pass

    def test_config_exists(self):
        assert self.config is not None

    def test_config_set_pragmatically(self):
        self.config["dummy"] = "hello_world"
        assert self.config["dummy"] is not None
        assert self.config["dummy"] == "hello_world"

    def test_config_reset(self):
        self.config["test_reset"] = "test"
        assert "authentication" in self.config
        self.config["authentication"]["hello"] = "world"
        assert self.config["test_reset"] == "test"
        assert self.config["authentication"]["hello"] == "world"
        self.config = polytope_config.ConfigParser().read(pytest.basic_config)
        assert "test_reset" not in self.config
        assert "hello" not in self.config["authentication"]

    def test_access_by_attribute_does_not_fail_silently(self):
        with pytest.raises(AttributeError):
            assert self.config.authentication is None

    def test_config_get_non_existant_key_fails(self):

        with pytest.raises(KeyError):
            assert self.config["_i_do_not_exist_"]

        with pytest.raises(KeyError):
            assert self.config["_i_do_not_exist_"]["abc"]

    def test_config_merge(self):
        merge = polytope_config.merge

        a = {"hello": "world"}
        b = {"hello": "world2"}
        c = {"bonjour": "le monde"}

        assert merge(a, b) == b
        assert merge(a, b) != a
        assert "bonjour" in merge(a, b, c)
        assert "hello" in merge(a, b, c)

        d = {"hello": ["world"]}
        e = {"hello": ["le monde"]}

        assert merge(d, e)["hello"] == ["world", "le monde"]

        f = {"one": {"two": {"three": 123}}}
        g = {"one": {"two": {"four": 456}}}

        assert merge(f, g)["one"]["two"] == {"three": 123, "four": 456}
