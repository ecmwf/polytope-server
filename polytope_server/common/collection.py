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
import logging

import yaml

from . import config as polytope_config
from .datasource import DataSource, coercion, create_datasource
from .exceptions import InvalidConfig
from .request import Request


class Collection:
    def __init__(self, name, config):

        self.config = config
        self.name = name
        self.roles = config.get("roles")
        self.limits = config.get("limits", {})
        self.ds_configs = []

        if len(self.config.get("datasources", [])) == 0:
            raise InvalidConfig("No datasources configured for collection {}".format(self.name))

        for ds_config in self.config.get("datasources"):
            # Allows passing in just the name as config
            if isinstance(ds_config, str):
                ds_config = {"name": ds_config}

            # 'name' means we are linking to a datasource defined in global_config.datasources
            if "name" in ds_config:
                name = ds_config["name"]
                datasource_configs = polytope_config.global_config.get("datasources")
                if name not in datasource_configs:
                    raise KeyError("Could not find config for datasource {}".format(name))
                # Merge with supplied config
                ds_config = polytope_config.merge(datasource_configs.get(name, None), ds_config)
            self.ds_configs.append(ds_config)

    def dispatch(self, request: Request) -> DataSource:
        """
        Match the request against the collection's datasources.
        Instantiates and returns the first matching datasource.
        """
        coerced_ur = coercion.coerce(yaml.safe_load(request.user_request))
        match_errors = []
        for ds_config in self.ds_configs:
            match_result = DataSource.match_ds(ds_config, coerced_ur, request.user)
            if match_result == "success":
                try:
                    message = f"Matched datasource {ds_config.get('repr', 'unknown')}\n"
                    request.user_message += message
                    logging.info(message.strip())
                    request.user_request = coerced_ur
                    logging.info("Final user request: {}".format(request.user_request))
                    ds = create_datasource(ds_config)
                    ds.dispatch(request, request.input_data)
                    return ds
                except Exception as e:
                    request.user_message += "Error creating datasource {}: {}\n".format(
                        ds_config.get("repr", "unknown"), str(e)
                    )
            else:
                match_errors.append(match_result)
        raise Exception("No matching datasource found for request {}".format(request.user_request))


def create_collections(config):
    collections = {}
    for k, v in config.items():
        collections[k] = Collection(k, v)
    return collections
