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
import json
import logging

import pytest
import requests

from polytope_server.common.staging import staging

proxies = {"http": None, "https": None}


class TestStaging:

    __test__ = False

    def setup_method(self, method):
        pass

    def test_staging_check_type(self):
        pass

    def test_staging_upload_download_binary(self):
        url = self.staging.create("test1", [self.binary_data], "application/octet-stream")
        assert "test1" in url
        result = self.staging.read("test1")
        assert self.binary_data == result

    def test_staging_upload_download_string(self):
        url = self.staging.create("test2", [self.string_data.encode()], "application/octet-stream")
        assert "test2" in url
        result = self.staging.read("test2")
        assert self.string_data == result.decode()

    def test_staging_upload_overwrites(self):
        url = self.staging.create("test3", [self.string_data.encode()], "application/octet-stream")
        assert "test3" in url
        result = self.staging.read("test3")
        assert self.string_data == result.decode()
        url = self.staging.create("test4", [self.string_data_2.encode()], "application/octet-stream")
        assert "test4" in url
        result = self.staging.read("test4")
        assert self.string_data_2 == result.decode()

    def test_staging_query_existence(self):
        data = b"I do exist!"
        self.staging.create("query_test", [data], "application/octet-stream")
        assert self.staging.query("query_test")

    def test_staging_query_non_existence(self):
        assert not self.staging.query("query_test_no_exist")

    def test_staging_stat(self):
        binary_data = b"12345"
        self.staging.create("sizeof", [binary_data], "application/octet-stream")
        typ, size = self.staging.stat("sizeof")
        assert size == 5

    def test_staging_stat_no_exist(self):
        with pytest.raises(KeyError):
            typ, size = self.staging.stat("size_test_no_exist")
            assert size == 5

    def test_staging_delete(self):
        self.staging.create("test_delete", [b"some data"], "application/octet-stream")
        assert self.staging.query("test_delete")
        self.staging.delete("test_delete")
        assert not self.staging.query("test_delete")

    def test_staging_delete_no_exist(self):
        with pytest.raises(KeyError):
            self.staging.delete("delete_test_no_exist")

    def test_staging_list(self):
        self.staging.create("test_list", [b"some data"], "application/octet-stream")
        resources = self.staging.list()
        logging.info(resources)
        assert resources[0].size > 0

        # Delete all objects listed
        for i in resources:
            self.staging.delete(i.name)

        resources = self.staging.list()
        assert len(resources) == 0

        self.staging.create("test_list_2", [b"some more data"], "application/octet-stream")
        resources = self.staging.list()
        assert len(resources) == 1
        assert resources[0].name == "test_list_2"
        assert resources[0].size == 14

    def test_staging_wipe(self):
        self.staging.wipe()
        assert len(self.staging.list()) == 0
        self.staging.create("test_wipe_1", [b"some more data"], "application/octet-stream")
        self.staging.create("test_wipe_2", [b"some more data"], "application/octet-stream")
        self.staging.create("test_wipe_3", [b"some more data"], "application/octet-stream")
        self.staging.create("test_wipe_4", [b"some more data"], "application/octet-stream")
        assert len(self.staging.list()) == 4
        self.staging.wipe()
        assert len(self.staging.list()) == 0
        self.staging.wipe()
        self.staging.wipe()

    def test_staging_upload_download_1byte(self):
        url = self.staging.create("test1", [b"1"], "application/octet-stream")
        assert "test1" in url
        result = self.staging.read("test1")
        assert b"1" == result

    def test_staging_upload_download_0byte(self):
        url = self.staging.create("test1", [b""], "application/octet-stream")
        assert "test1" in url
        result = self.staging.read("test1")
        assert b"" == result

    # Testing plain old python requests (used for external access)

    def test_staging_upload_get(self):
        url = self.staging.create("test1", [self.binary_data], "application/octet-stream")
        assert "test1" in url

        # Should be able to curl with no credentials
        result = requests.get(url, proxies=proxies)
        assert self.binary_data == result.content

    def test_staging_list_objects_denied(self):
        response = requests.get("http://{}:{}/{}".format(self.host, self.port, self.bucket), proxies=proxies)
        logging.info(response)
        assert response.status_code == 403

    def test_staging_get_url_denied(self):
        response = requests.get("http://{}:{}".format(self.host, self.port), proxies=proxies)
        logging.info(response)
        assert response.status_code == 403

    def test_staging_get_string_data(self):
        data = {"hello": "world"}
        json_data = json.dumps(data)
        url = self.staging.create("json_data.json", [json_data.encode("utf-8")], "application/octet-stream")
        result = requests.get(url, proxies=proxies)
        assert data == json.loads(result.content.decode("utf-8"))

    def test_staging_get_grib_data(self):
        data = b"I am a grib file"
        url = self.staging.create("data.grib", [data], "application/x-grib")
        result = requests.get(url, proxies=proxies)
        assert result.headers["content-type"] == "application/x-grib"
        assert result.content == data


@pytest.mark.staging_s3
class TestS3Staging(TestStaging):

    __test__ = True

    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config)

        self.staging_config = config.get("staging")
        self.staging_config["s3"]["bucket"] = "testing"
        self.staging_config["s3"]["url"] = (
            "http://" + self.staging_config["s3"]["host"] + ":" + str(self.staging_config["s3"]["port"])
        )
        self.staging = staging.create_staging({"s3": self.staging_config.get("s3")})

        self.host = self.staging.host
        self.port = self.staging.port
        self.bucket = self.staging.bucket

        self.binary_data = b"abc123"
        self.string_data = "xyz789"
        self.string_data_2 = "xyz123"

    # Testing the internal interface

    def test_staging_check_type(self):
        assert self.staging.get_type() == "S3DataStaging"


@pytest.mark.staging_polytope
class TestPolytopeStaging(TestStaging):

    __test__ = True

    def setup_method(self, method):

        config = copy.deepcopy(pytest.polytope_config)

        self.staging_config = config.get("staging")
        self.staging_config["polytope"]["url"] = (
            "http://" + self.staging_config["polytope"]["host"] + ":" + str(self.staging_config["polytope"]["port"])
        )
        self.staging = staging.create_staging({"polytope": self.staging_config.get("polytope")})

        self.host = self.staging.host
        self.port = self.staging.port
        self.bucket = ""

        self.binary_data = b"abc123"
        self.string_data = "xyz789"
        self.string_data_2 = "xyz123"

    def test_staging_check_type(self):
        assert self.staging.get_type() == "PolytopeStaging"

    @pytest.mark.skip(reason="Polytope staging does not have a wipe method")
    def test_staging_wipe(self):
        pass

    @pytest.mark.skip(reason="Polytope staging is insecure, this test fails")
    def test_staging_get_url_denied(self):
        pass

    @pytest.mark.skip(reason="Polytope staging is insecure, this test fails")
    def test_staging_list_objects_denied(self):
        pass
