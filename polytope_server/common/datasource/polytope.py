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
import os

import yaml
from polytope_feature.utility.exceptions import PolytopeError
from polytope_mars.api import PolytopeMars

from ..request import Request
from ..schedule import SCHEDULE_READER
from . import datasource


class PolytopeDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        assert self.type == "polytope"
        self.match_rules = config.get("match", {})
        self.pre_path = config.get("options", {}).pop("pre_path", [])
        self.patch_rules = config.get("patch", {})
        self.defaults = config.get("defaults", {})
        self.extra_required_role = config.get("extra_required_role", {})
        # https://github.com/ecmwf/polytope-server/issues/68
        self.gh68_fix_hashes = config.get("gh68_fix_hashes", False)
        # https://github.com/ecmwf/polytope-server/issues/69
        self.gh69_fix_grids = config.get("gh69_fix_grids", False)
        # https://github.com/ecmwf/polytope-server/issues/70
        self.gh70_fix_step_ranges = config.get("gh70_fix_step_ranges", False)
        self.separate_datetime = config.get("separate_datetime", False)
        self.obey_schedule = config.get("obey_schedule", False)
        self.output = None

        # Create a temp file to store gribjump config
        self.config_file = "/tmp/gribjump.yaml"
        with open(self.config_file, "w") as f:
            f.write(yaml.dump(self.config.pop("gribjump_config")))
        self.config["datacube"]["config"] = self.config_file
        os.environ["GRIBJUMP_CONFIG_FILE"] = self.config_file

        # Create a temp file to store FDB config
        self.fdb_config_file = "/tmp/fdb.yaml"
        if "fdb_config" in config:
            with open(self.fdb_config_file, "w") as f:
                f.write(yaml.dump(self.config.pop("fdb_config")))
            os.environ["FDB5_CONFIG_FILE"] = self.fdb_config_file

        logging.info("Set up gribjump")

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def check_extra_roles(self, request: Request) -> bool:
        # if the user has any of the extra roles, they are allowed
        realm = request.user.realm
        req_extra_roles = self.extra_required_role.get(realm, [])

        if len(req_extra_roles) == 0:
            return True

        logging.info(f"Checking for user roles in required extra roles: {req_extra_roles}")
        logging.info(f"User roles: {request.user.roles}")

        if any(role in req_extra_roles for role in request.user.roles):
            return True
        else:
            return False

    def retrieve(self, request):
        r = copy.deepcopy(request.user_request)
        # Downstream expects MARS-like format of request
        for key in r:
            if isinstance(r[key], list):
                r[key] = "/".join(r[key])

        # Check data released
        if SCHEDULE_READER is not None and self.obey_schedule:
            SCHEDULE_READER.check_released_polytope_request(r)

        r = self.apply_defaults(r)

        logging.info(r)

        # Set the "pre-path" for this request
        pre_path = {}
        for k, v in r.items():
            v = v.split("/") if isinstance(v, str) else v
            if k in self.pre_path:
                if isinstance(v, list):
                    if self.gh70_fix_step_ranges:
                        if k == "param":
                            pre_path[k] = v[0]
                    if len(v) == 1:
                        v = v[0]
                        pre_path[k] = v

        polytope_mars_config = copy.deepcopy(self.config)
        polytope_mars_config["options"]["pre_path"] = pre_path

        if self.gh69_fix_grids:
            change_grids(r, polytope_mars_config)
        if self.gh68_fix_hashes:
            change_hash(r, polytope_mars_config)
        if self.separate_datetime:
            unmerge_date_time_options(r, polytope_mars_config)

        polytope_mars = PolytopeMars(
            polytope_mars_config,
            log_context={
                "user": request.user.realm + ":" + request.user.username,
                "id": request.id,
            },
        )

        try:
            self.output = polytope_mars.extract(r)
            self.output = json.dumps(self.output).encode("utf-8")
        except PolytopeError as e:
            raise Exception("Polytope Feature Extraction Error: {}".format(e.message))

        return True

    def result(self, request):
        logging.info("Getting result")
        yield self.output

    def destroy(self, request) -> None:
        # delete temp files
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        if os.path.exists(self.fdb_config_file):
            os.remove(self.fdb_config_file)
        pass

    def repr(self):
        return self.config.get("repr", "polytope")

    def mime_type(self) -> str:
        return "application/prs.coverage+json"

    def apply_defaults(self, request):
        request = copy.deepcopy(request)
        for k, v in self.defaults.items():
            if k not in request:
                request[k] = v
        return request


def change_grids(request, config):
    """
    Temporary fix for request-dependent grid changes in polytope
    see https://github.com/ecmwf/polytope-server/issues/69
    """
    res = None

    # This only holds for climate dt data
    if request.get("dataset", None) == "climate-dt":
        # all resolution=standard have h128
        if request["resolution"] == "standard":
            res = 128
            return change_config_grid_res(config, res)

        # for activity CMIP6 and experiment hist, all models except ifs-nemo have h512 and ifs-nemo has h1024
        if request["activity"] == "cmip6" and request["experiment"] == "hist":
            res = 1024

        # # for activity scenariomip and experiment ssp3-7.0, all models use h1024
        # if request["activity"] == "scenariomip" and request["experiment"] == "ssp3-7.0":
        #     res = 1024

        if request["activity"] == "story-nudging":
            res = 512

        if request["activity"] in ["baseline", "projections", "scenariomip"]:
            res = 1024

    elif request.get("dataset", None) == "extremes-dt":
        if request["stream"] == "wave":
            for mappings in config["options"]["axis_config"]:
                for sub_mapping in mappings["transformations"]:
                    if sub_mapping["name"] == "mapper":
                        sub_mapping["type"] = "reduced_ll"
                        sub_mapping["resolution"] = 3601
            return config

    elif request.get("class", None) == "ai":
        for mappings in config["options"]["axis_config"]:
            for sub_mapping in mappings["transformations"]:
                if sub_mapping["name"] == "mapper":
                    sub_mapping["resolution"] = 320
                    sub_mapping["type"] = "reduced_gaussian"
        return config

    # Only assign new resolution if it was changed here
    if res:
        # Find the mapper transformation
        return change_config_grid_res(config, res)

    return config


def change_hash(request, config):
    """
    Temporary fix for grid mismatch in polytope
    see https://github.com/ecmwf/polytope-server/issues/68
    """

    # This only holds for operational data
    if request.get("dataset", None) is None:
        if request["levtype"] == "ml":
            hash = "9fed647cd1c77c03f66d8c74a4e0ad34"
            return change_config_grid_hash(config, hash)
    if request.get("dataset", None) is None:
        if request["stream"] == "enfo":
            if request["class"] == "od":
                if request["type"] == "pf":
                    if request["param"] == "261001":
                        hash = "6101cfb6f4671e41e5cb93fe9596065b"
                        return change_config_grid_hash(config, hash)
    if request.get("dataset", None) == "climate-dt":
        if request.get("model", None) == "icon":
            hash = "9533855ee8e38314e19aaa0434c310da"
            return change_config_grid_hash(config, hash)
    return config


def change_config_grid_hash(config, hash):
    for mappings in config["options"]["axis_config"]:
        for sub_mapping in mappings["transformations"]:
            if sub_mapping["name"] == "mapper":
                sub_mapping["md5_hash"] = hash
    return config


def change_config_grid_res(config, res):
    for mappings in config["options"]["axis_config"]:
        for sub_mapping in mappings["transformations"]:
            if sub_mapping["name"] == "mapper":
                sub_mapping["resolution"] = res
    return config


def unmerge_date_time_options(request, config):
    if request.get("dataset", None) == "climate-dt" and (
        request["feature"]["type"] == "timeseries" or request["feature"]["type"] == "polygon"
    ):
        for mappings in config["options"]["axis_config"]:
            if mappings["axis_name"] == "date":
                mappings["transformations"] = [{"name": "type_change", "type": "date"}]
        config["options"]["axis_config"].append(
            {"axis_name": "time", "transformations": [{"name": "type_change", "type": "time"}]}
        )
    return config
