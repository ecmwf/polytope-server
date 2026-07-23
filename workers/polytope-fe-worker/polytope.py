# SPDX-FileCopyrightText: 2022 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import logging
import os

import yaml


class PolytopeDataSource:
    def __init__(self, config):
        self.config = config
        self.type = config["type"]
        assert self.type == "polytope"
        self.pre_path = config.get("options", {}).pop("pre_path", [])
        self.defaults = config.get("defaults", {})
        # https://github.com/ecmwf/polytope-server/issues/68
        self.gh68_fix_hashes = config.get("gh68_fix_hashes", False)
        # https://github.com/ecmwf/polytope-server/issues/69
        self.gh69_fix_grids = config.get("gh69_fix_grids", False)
        # https://github.com/ecmwf/polytope-server/issues/70
        self.gh70_fix_step_ranges = config.get("gh70_fix_step_ranges", False)
        self.separate_datetime = config.get("separate_datetime", False)
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

    def retrieve(self, request):
        import time
        from polytope_feature.utility.exceptions import PolytopeError
        from polytope_mars.api import PolytopeMars

        t0 = time.monotonic()

        r = copy.deepcopy(request.coerced_request)

        for k, v in list(r.items()):
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                r[k] = str(v)

        # Start with a per-request deepcopy of the base config. The base pool
        # config may carry no "options" at all (a single FE pool that selects the
        # per-dataset datacube/options at routing time via job metadata), so do
        # not assume "options" exists.
        polytope_mars_config = copy.deepcopy(self.config)
        polytope_mars_config.setdefault("options", {})

        # The pre_path axis list comes from the base pool config (self.pre_path,
        # popped from options in __init__). A trusted metadata block may replace
        # the whole datacube/options for this dataset, including its own pre_path
        # axis list, so prefer the metadata-supplied list when present.
        pre_path_axes = self.pre_path

        # Merge trusted metadata options if present (written only by the broker
        # set_metadata action; never sourced from the client request).
        metadata_polytope_mars = request.metadata.get("polytope_mars")
        if metadata_polytope_mars is not None:
            if not isinstance(metadata_polytope_mars, dict):
                raise ValueError(
                    f"request.metadata['polytope_mars'] must be a dict, got {type(metadata_polytope_mars).__name__}"
                )
            # Overlay only allowed structural keys: datacube and options
            if "datacube" in metadata_polytope_mars:
                polytope_mars_config["datacube"] = metadata_polytope_mars["datacube"]
            if "options" in metadata_polytope_mars:
                if not isinstance(metadata_polytope_mars["options"], dict):
                    raise ValueError(
                        "request.metadata['polytope_mars']['options'] must be a dict"
                    )
                merged_options = copy.deepcopy(metadata_polytope_mars["options"])
                # pre_path in an options block is a list of axis names; pop it so
                # it is not left as a list where the per-request dict is expected.
                if "pre_path" in merged_options:
                    pre_path_axes = merged_options.pop("pre_path")
                polytope_mars_config["options"].update(merged_options)

        # Build the per-request pre_path dict from the selected axis list (works
        # for both the metadata path and the backward-compatible base-config path).
        pre_path = {}
        for k, v in r.items():
            v = v.split("/") if isinstance(v, str) else v
            if k in pre_path_axes:
                if isinstance(v, list):
                    if self.gh70_fix_step_ranges:
                        if k == "param":
                            pre_path[k] = v[0]
                    if len(v) == 1:
                        v = v[0]
                        pre_path[k] = v
        polytope_mars_config["options"]["pre_path"] = pre_path

        if self.gh69_fix_grids:
            change_grids(r, polytope_mars_config)
        if self.gh68_fix_hashes:
            change_hash(r, polytope_mars_config)
        if self.separate_datetime:
            unmerge_date_time_options(r, polytope_mars_config)

        t_coerce = time.monotonic()

        polytope_mars = PolytopeMars(
            polytope_mars_config,
            log_context={
                "user": request.user.realm + ":" + request.user.username,
                "id": request.id,
            },
        )

        t_mars_init = time.monotonic()

        try:
            self.output = polytope_mars.extract(r)
            t_extract = time.monotonic()
            self.output = json.dumps(self.output).encode("utf-8")
            t_encode = time.monotonic()
        except PolytopeError as e:
            raise Exception("Polytope Feature Extraction Error: {}".format(e.message))

        return {
            "coerce_ms": round((t_coerce - t0) * 1000, 1),
            "mars_init_ms": round((t_mars_init - t_coerce) * 1000, 1),
            "extract_ms": round((t_extract - t_mars_init) * 1000, 1),
            "encode_ms": round((t_encode - t_extract) * 1000, 1),
            "retrieve_ms": round((t_encode - t0) * 1000, 1),
        }

    def result(self, request):
        logging.debug("Getting result")
        yield self.output

    def destroy(self, request) -> None:
        # These files are created once with this process-scoped datasource and
        # reused by every job. Removing them here breaks all subsequent jobs.
        pass

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
            elif (
                request["activity"] == "highresmip" and request["model"] == "ifs-fesom"
            ):
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
    if (
        request.get("dataset", None) == "climate-dt"
        or request.get("class", None) == "ng"
    ) and (
        request["feature"]["type"] == "timeseries"
        or request["feature"]["type"] == "polygon"
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
