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
import sys
import tempfile
from importlib import import_module
from pathlib import Path

import yaml
from covjsonkit.param_db import get_param_id_from_db
from polytope_feature.utility.exceptions import PolytopeError

from ..request import PolytopeRequest
from ..schedule import SCHEDULE_READER
from . import datasource


def _import_polytope_mars():
    for module_name in list(sys.modules):
        if (
            module_name == "polytope_mars"
            or module_name.startswith("polytope_mars.")
            or module_name == "pygribjump"
            or module_name.startswith("pygribjump.")
            or module_name == "pyfdb"
            or module_name.startswith("pyfdb.")
        ):
            sys.modules.pop(module_name, None)
    return import_module("polytope_mars.api").PolytopeMars


def _python_site_packages(bundle_root: Path) -> Path:
    return bundle_root / ".venv" / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"


class PolytopeDataSource(datasource.DataSource):
    def __init__(self, config):
        self.config = copy.deepcopy(config)
        self.type = config["type"]
        assert self.type == "polytope"
        self.pre_path = self.config.get("options", {}).pop("pre_path", [])
        self.defaults = self.config.get("defaults", {})
        # https://github.com/ecmwf/polytope-server/issues/68
        self.gh68_fix_hashes = self.config.get("gh68_fix_hashes", False)
        # https://github.com/ecmwf/polytope-server/issues/69
        self.gh69_fix_grids = self.config.get("gh69_fix_grids", False)
        # https://github.com/ecmwf/polytope-server/issues/70
        self.gh70_fix_step_ranges = self.config.get("gh70_fix_step_ranges", False)
        self.separate_datetime = self.config.get("separate_datetime", False)
        self.obey_schedule = self.config.get("obey_schedule", False)
        self.output = None
        self._saved_env = {}
        self._inserted_sys_path = None
        self._source_bundle_root = self.config.get("source_bundle_root")

        # Create a temp file to store gribjump config
        self._save_env("GRIBJUMP_CONFIG_FILE")
        with tempfile.NamedTemporaryFile(mode="w", prefix="gribjump-", suffix=".yaml", delete=False) as f:
            self.config_file = f.name
            f.write(yaml.dump(self.config.pop("gribjump_config")))
        self.config["datacube"]["config"] = self.config_file
        os.environ["GRIBJUMP_CONFIG_FILE"] = self.config_file

        # Create a temp file to store FDB config
        self.fdb_config_file = None
        if "fdb_config" in self.config:
            self._save_env("FDB5_CONFIG_FILE")
            with tempfile.NamedTemporaryFile(mode="w", prefix="fdb-", suffix=".yaml", delete=False) as f:
                self.fdb_config_file = f.name
                f.write(yaml.dump(self.config.pop("fdb_config")))
            os.environ["FDB5_CONFIG_FILE"] = self.fdb_config_file

        if self._source_bundle_root:
            self._activate_source_bundle(Path(self._source_bundle_root))

    def _save_env(self, name: str) -> None:
        if name not in self._saved_env:
            self._saved_env[name] = os.environ.get(name)

    def _activate_source_bundle(self, bundle_root: Path) -> None:
        site_packages = _python_site_packages(bundle_root)
        bundle_lib = str(bundle_root / "lib")
        logging.info(
            "Activating source-built GribJump bundle",
            extra={
                "source_bundle_root": str(bundle_root),
                "source_bundle_site_packages": str(site_packages),
            },
        )
        self._save_env("GRIBJUMP_HOME")
        self._save_env("FDB_HOME")
        self._save_env("FDB5_HOME")
        self._save_env("GRIBJUMP_DIR")
        self._save_env("FDB5_DIR")
        self._save_env("ECCODES_DIR")
        self._save_env("FINDLIBS_DISABLE_PACKAGE")
        self._save_env("LD_LIBRARY_PATH")

        os.environ["GRIBJUMP_HOME"] = str(bundle_root)
        os.environ["FDB_HOME"] = str(bundle_root)
        os.environ["FDB5_HOME"] = str(bundle_root)
        os.environ["GRIBJUMP_DIR"] = str(bundle_root)
        os.environ["FDB5_DIR"] = str(bundle_root)
        os.environ["ECCODES_DIR"] = str(bundle_root)
        os.environ["FINDLIBS_DISABLE_PACKAGE"] = "yes"

        ld_library_path = os.environ.get("LD_LIBRARY_PATH")
        if ld_library_path:
            os.environ["LD_LIBRARY_PATH"] = f"{bundle_lib}:{ld_library_path}"
        else:
            os.environ["LD_LIBRARY_PATH"] = bundle_lib

        if str(site_packages) not in sys.path:
            sys.path.insert(0, str(site_packages))
            self._inserted_sys_path = str(site_packages)

    def _restore_env(self) -> None:
        for name, value in self._saved_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        if self._inserted_sys_path and self._inserted_sys_path in sys.path:
            sys.path.remove(self._inserted_sys_path)
            self._inserted_sys_path = None

    def get_type(self):
        return self.type

    def archive(self, request):
        raise NotImplementedError()

    def retrieve(self, request):
        r = copy.deepcopy(request.coerced_request)

        # Check data released
        if SCHEDULE_READER is not None and self.obey_schedule:
            SCHEDULE_READER.check_released_polytope_request(r)

        # Set the "pre-path" for this request
        pre_path = {}
        for k, v in r.items():
            v = v.split("/") if isinstance(v, str) else v
            if k in self.pre_path:
                if isinstance(v, list):
                    if self.gh70_fix_step_ranges:
                        if k == "param" and not str(v[0]).lstrip("-").isdigit():
                            try:
                                v[0] = get_param_id_from_db(v[0])
                            except Exception:
                                logging.warning(
                                    "Could not convert param shortname '%s' to param id",
                                    v[0],
                                )
                            pre_path[k] = v[0]
                    if len(v) == 1:
                        v = v[0]
                        if k == "param" and not str(v).lstrip("-").isdigit():
                            try:
                                v = get_param_id_from_db(v)
                            except Exception:
                                logging.warning(
                                    "Could not convert param shortname '%s' to param id",
                                    v,
                                )
                        pre_path[k] = v

        polytope_mars_config = copy.deepcopy(self.config)
        polytope_mars_config["options"]["pre_path"] = pre_path

        if self.gh69_fix_grids:
            change_grids(r, polytope_mars_config)
        if self.gh68_fix_hashes:
            change_hash(r, polytope_mars_config)
        if self.separate_datetime:
            unmerge_date_time_options(r, polytope_mars_config)

        PolytopeMars = _import_polytope_mars()
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
        logging.debug("Getting result")
        yield self.output

    def destroy(self, request) -> None:
        # delete temp files
        if os.path.exists(self.config_file):
            os.remove(self.config_file)
        if self.fdb_config_file and os.path.exists(self.fdb_config_file):
            os.remove(self.fdb_config_file)
        self._restore_env()

    def mime_type(self) -> str:
        return "application/prs.coverage+json"


def change_grids(request, config):
    """
    Temporary fix for request-dependent grid changes in polytope
    see https://github.com/ecmwf/polytope-server/issues/69
    """
    res = None

    if request.get("class", None) == "ng":
        # all resolution=standard have h128
        if request["resolution"] == "standard":
            res = 128
            return change_config_grid_res(config, res)

    # This only holds for climate dt data
    elif request.get("dataset", None) == "climate-dt":
        if request["resolution"] == "standard":
            res = 128

        else:
            # activity scenariomip is only used in generation 1 so don't need to specify generation 1 here
            # This is the nextgems under class d1 run in generation 1
            # In generation 2, runs with realization 2,3,4 used H512.
            if request["realization"] != "1":
                res = 512

            # IFS-FESOM under generation 1 highresmip is H512
            elif request["activity"] == "highresmip" and request["model"] == "ifs-fesom":
                res = 512

            # All story nudging runs here are H512
            elif request["activity"] == "story-nudging":
                res = 512

            # Catch all for others that don't fit into exception cases above
            elif request["activity"] in [
                "baseline",
                "cmip6",
                "highresmip",
                "projections",
                "scenariomip",
            ]:
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
        if request.get("resolution", None) == "high":
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
    if (request.get("dataset", None) == "climate-dt" or request.get("class", None) == "ng") and (
        request["feature"]["type"] == "timeseries" or request["feature"]["type"] == "polygon"
    ):
        for mappings in config["options"]["axis_config"]:
            if mappings["axis_name"] == "date":
                mappings["transformations"] = [{"name": "type_change", "type": "date"}]
        config["options"]["axis_config"].append(
            {
                "axis_name": "time",
                "transformations": [{"name": "type_change", "type": "time"}],
            }
        )
    return config
