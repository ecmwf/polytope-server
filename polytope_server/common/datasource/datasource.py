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

# import traceback
from abc import ABC
from importlib import import_module
from typing import Iterator

from .. import config as polytope_config
from ..request import Verb

#######################################################


class DataSource(ABC):
    """An ephemeral connection to a datasource, which is generated for each request.
    Stores state relating to the request, including data, errors, etc."""

    def __init__(self, config):
        """Instantiate a datasource"""
        raise NotImplementedError()

    def archive(self, request: str) -> None:
        """Archive data, returns nothing but updates datasource state"""
        raise NotImplementedError()

    def retrieve(self, request: str) -> None:
        """Retrieve data, returns nothing but updates datasource state"""
        raise NotImplementedError()

    def match(self, request: str) -> tuple[bool, str]:
        """Checks if the request matches the datasource, returns a tuple (match: bool, message: str)"""
        raise NotImplementedError()

    def repr(self) -> str:
        """Returns a string name of the datasource, presented to the user on error"""
        raise NotImplementedError

    def get_type(self) -> str:
        """Returns a string stating the type of this object (e.g. fdb, mars, echo)"""
        raise NotImplementedError()

    def result(self, request) -> Iterator[bytes]:
        """Returns a generator for the resultant data"""
        raise NotImplementedError()

    def destroy(self, request) -> None:
        """A hook to do essential freeing of resources, called upon success or failure"""
        raise NotImplementedError()

    def mime_type(self) -> str:
        """Returns the mimetype of the result"""
        raise NotImplementedError()

    def dispatch(self, request, input_data) -> tuple[bool, bool, str]:
        """
        Dispatch to match, retrieve and archive.
        Returns a tuple ( match, success, details )
            match: bool
            success: bool
            details: Messages to be passed back to user. Empty string if success.
        This is the main entry point for the datasource.
        """

        self.input_data = input_data

        # Match
        ds_match = False
        try:
            ds_match, message = self.match(request)
            logging.info(f"Datasource {self.repr()} match: {ds_match}, message: {message}")
            if not ds_match:
                return (False, False, f"Skipping datasource {self.repr()}: {message}\n")
        except Exception as e:
            logging.exception(f"Error matching datasource {self.repr()}: {repr(e)}", exc_info=True, stack_info=True)
            return (False, f"Skipping datasource {self.repr()}: error matching: {repr(e)}\n")
        # Check for datasource-specific roles
        if hasattr(self, "config"):
            datasource_role_rules = self.config.get("roles", None)
            if datasource_role_rules is not None:
                if not any(role in request.user.roles for role in datasource_role_rules.get(request.user.realm, [])):
                    return (ds_match, False, f"Skipping datasource {self.repr()}: user is not authorised.\n")

        # Retrieve/Archive/etc.
        success = False
        try:
            if request.verb == Verb.RETRIEVE:
                success = self.retrieve(request)
            elif request.verb == Verb.ARCHIVE:
                success = self.archive(request)
            else:
                return (
                    ds_match,
                    success,
                    f" Datasource {self.repr()} matched, but method '{request.verb}' not available. \n",
                )
        except Exception as e:
            logging.exception(
                f"Error while sending {request.verb} request with datasource {self.repr()}: {repr(e)}",
                exc_info=True,
                stack_info=True,
            )
            return (
                ds_match,
                success,
                f"Datasource {self.repr()} matched, but ran into an error while sending data request: {repr(e)}\n",
            )

        return (ds_match, success, "Successfully retrieved data from datasource {}\n".format(self.repr()))


#######################################################

type_to_class_map = {
    "fdb": "FDBDataSource",
    "mars": "MARSDataSource",
    "webmars": "WebMARSDataSource",
    "polytope": "PolytopeDataSource",
    "federated": "FederatedDataSource",
    "echo": "EchoDataSource",
    "dummy": "DummyDataSource",
    "raise": "RaiseDataSource",
    "ionbeam": "IonBeamDataSource",
}


def create_datasource(config) -> type[DataSource]:

    # Allows passing in just the name as config
    if isinstance(config, str):
        config = {"name": config}

    # 'name' means we are linking to a datasource defined in global_config.datasources
    if "name" in config:
        name = config["name"]
        datasource_configs = polytope_config.global_config.get("datasources")
        if name not in datasource_configs:
            raise KeyError("Could not find config for datasource {}".format(name))
        # Merge with supplied config
        config = polytope_config.merge(datasource_configs.get(name, None), config)

    # Find the class matching config.type
    type = config.get("type")
    module = import_module("polytope_server.common.datasource." + type)
    datasource_class = type_to_class_map[type]

    # Call the constructor
    constructor = getattr(module, datasource_class)
    datasource = constructor(config)

    logging.info("Datasource {} initialized [{}].".format(type, datasource_class))

    return datasource
