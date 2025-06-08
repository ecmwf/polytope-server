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
from abc import ABC
from importlib import import_module
from typing import Any, Dict, Iterator

from ..auth import AuthHelper
from ..date_check import DateError, date_check
from ..exceptions import ForbiddenRequest
from ..request import Request, Verb
from ..user import User

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

    def retrieve(self, request: Request) -> None:
        """Retrieve data, returns nothing but updates datasource state"""
        raise NotImplementedError()

    @staticmethod
    def match(ds_config, coerced_ur: Dict[str, Any], user: User) -> str:
        """
        Match the request against a specific datasource configuration.

        Checks if the user is authorized, applies defaults, and checks match rules.
        Includes datasource-specific checks based on the type of datasource in the config.

        :param ds_config: The datasource configuration to match against.
        :param coerced_ur: The coerced user request. This may be modified by applying defaults.
        :param user: The user making the request.
        :return: str: "success" if the request matches the datasource, or an error message if it does not.
        """
        # check datasource specific roles
        roles = ds_config.get("roles", [])
        try:
            if roles and not AuthHelper.is_authorized(user, roles):
                return f"Skipping datasource {DataSource.repr(ds_config)}: user not authorized."
        except ForbiddenRequest as e:
            message = f"Skipping datasource {DataSource.repr(ds_config)}: {repr(e)}"
            logging.warning(message)
            return message

        # apply defaults
        defaults = ds_config.get("defaults", {})
        if "date" not in defaults:
            defaults["date"] = "-1"  # today, default for mars
        for k, v in defaults.items():
            if k not in coerced_ur:
                coerced_ur[k] = v

        # check match rules
        if ds_config.get("type") == "polytope":
            if "feature" not in coerced_ur:
                return (
                    f"Skipping datasource {DataSource.repr(ds_config)}: "
                    "request does not contain expected key 'feature'"
                )
        elif "feature" in coerced_ur:
            return (
                f"Skipping datasource {DataSource.repr(ds_config)}: "
                "request contains key 'feature', but this is not expected by the datasource."
            )
        match_rules = ds_config.get("match", {})
        for rule_key, allowed_values in match_rules.items():

            # An empty match rule means that the key must not be present
            if allowed_values is None or len(allowed_values) == 0:
                if rule_key in coerced_ur:
                    return (
                        f"Skipping datasource {DataSource.repr(ds_config)}: "
                        f"request containing key '{rule_key}' is not allowed."
                    )
                else:
                    continue  # no more checks to do

            # Check that the required key exists
            if rule_key not in coerced_ur:
                return (
                    f"Skipping datasource {DataSource.repr(ds_config)}: "
                    f"request does not contain expected key '{rule_key}'"
                )

            # Process date rules
            if rule_key == "date":
                try:
                    date_check(coerced_ur["date"], allowed_values)
                except DateError as e:
                    return f"Skipping datasource {DataSource.repr(ds_config)}: {e}."
                except Exception as e:
                    return f"Skipping datasource {DataSource.repr(ds_config)}: error processing date check: {e}."
                continue

            # check that all values in request are allowed
            allowed_values = [allowed_values] if not isinstance(allowed_values, (list, tuple)) else allowed_values
            request_values = (
                [coerced_ur[rule_key]] if not isinstance(coerced_ur[rule_key], (list, tuple)) else coerced_ur[rule_key]
            )
            if not set(request_values).issubset(set(allowed_values)):
                return (
                    f"Skipping datasource {DataSource.repr(ds_config)}: "
                    f"got {rule_key} : {coerced_ur[rule_key]}, but expected one of {allowed_values}"
                )
        # If we reach here, the request matches the datasource
        # Downstream expects MARS-like format of request
        for key in coerced_ur:
            if isinstance(coerced_ur[key], list):
                coerced_ur[key] = "/".join(coerced_ur[key])
        return "success"

    @staticmethod
    def repr(config) -> str:
        """Returns a string name of the datasource, presented to the user on error"""
        return config.get("repr", config.get("name", config.get("type", "unknown")))

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

    def dispatch(self, request: Request, input_data) -> bool:
        """
        Dispatch to retrieve or archive.
        This is the main entry point for the datasource, called by the worker/collection.
        It adds information to the request.user_message.
        Returns
            success: bool
        """

        self.input_data = input_data

        # Retrieve/Archive/etc.
        success = False
        try:
            if request.verb == Verb.RETRIEVE:
                success = self.retrieve(request)
            elif request.verb == Verb.ARCHIVE:
                success = self.archive(request)
            else:
                raise NotImplementedError()

        except NotImplementedError as e:
            request.user_message += "Skipping datasource {}: method '{}' not available: {}\n".format(
                self.repr(self.config), request.verb, repr(e)
            )
            return False

        return success


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


def create_datasource(config) -> DataSource:

    # Find the class matching config.type
    type = config.get("type")
    module = import_module("polytope_server.common.datasource." + type)
    datasource_class = type_to_class_map[type]

    # Call the constructor
    constructor = getattr(module, datasource_class)
    datasource = constructor(config)

    logging.info("Datasource {} initialized [{}].".format(type, datasource_class))

    return datasource


def convert_to_mars_request(request, verb=None):
    """
    Converts a Python dictionary to a MARS request string.
    If verb is provided, it is prepended to the request string (e.g., 'retrieve').
    """
    parts = []
    if verb:
        parts.append(verb)
    for k, v in request.items():
        if isinstance(v, (list, tuple)):
            v = "/".join(str(x) for x in v)
        else:
            v = str(v)
        parts.append(f"{k}={v}")
    return ",".join(parts)
