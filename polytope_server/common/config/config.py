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

import argparse
import copy
import logging
import os
import sys

import hiyapyco
import pykwalify
import yaml
from pykwalify.core import Core

from .. import config as polytope_config


def _merge(a, b, path=None):
    "merges dict b into dict a"

    a = copy.deepcopy(a)
    b = copy.deepcopy(b)

    if path is None:
        path = []

    if not isinstance(b, dict):
        if isinstance(a, list) and isinstance(b, list):
            return a + b
        return b

    for key in b:
        if key in a:
            if b[key] is None:
                del a[key]
            elif isinstance(a[key], dict) and isinstance(b[key], dict):
                a[key] = _merge(a[key], b[key], path + [str(key)])
            elif isinstance(a[key], list) and isinstance(b[key], list):
                a[key] = a[key] + b[key]
            elif a[key] == b[key]:
                pass
            else:
                # raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
                a[key] = b[key]
        else:
            if b[key] is not None:
                a[key] = b[key]
    return a


def merge(*configs):
    new = {}
    for c in configs:
        new = _merge(new, c)
        # new = deepmerge.always_merger.merge(new, c)
    return new


class ConfigParser:
    def read(self, additional_yaml=None):
        """Read the configuration from YAML and return it as an attribute-dict"""
        self._parse_args()
        self._read_yaml(additional_yaml=additional_yaml)
        self.config = self._interpolate_env_vars(self.config)
        self._check_schema()
        polytope_config.global_config = self.config
        return self.config

    def unused_cli_args(self):
        """Return any unused command-line arguments remaining after ingesting configuration"""
        return self.cli_unknown_args

    def list_config_files(self):
        """List the files that were used by read()"""
        return self.yaml_files

    def dump(self):
        """Dumps the final, merged config created by read()"""
        return hiyapyco.dump(self.config)

    def _parse_args(self):
        """Parses the command line arguments for config (-f)"""

        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-f",
            "--config",
            dest="config_files",
            action="append",
            help="specify a yaml file containing config values to be appended, may be specified multiple times",
        )
        self.cli_args, self.cli_unknown_args = parser.parse_known_args(namespace=None)

    def _read_yaml(self, additional_yaml=None):
        """Reads and merges yaml config files"""

        self.yaml_files = additional_yaml or []

        # If it exists, use the base YAML from /etc/polytope/config.yaml
        _config_file = "/etc/polytope/config.yaml"
        if os.path.isfile(_config_file):
            self.yaml_files.append(_config_file)

        # Overlay anything passed via the command line
        if self.cli_args.config_files is not None:
            for f in self.cli_args.config_files:
                self.yaml_files.append(f)

        if len(self.yaml_files) == 0:
            raise Exception("No configuration files specified, use -f [config] on the command line.")

        # Merge
        configs = []
        for c in self.yaml_files:
            with open(c, "r") as f:
                r = yaml.load_all(f, Loader=yaml.FullLoader)
                for i in r:
                    configs.append(i)
        self.config = merge(*configs)

        # Merge with hiyapyco
        # self.config = hiyapyco.load( self.yaml_files, usedefaultyamlloader=False,
        # method=hiyapyco.METHOD_MERGE, loglevel='WARN' )

    def _check_schema(self):
        """Validates the configuration against the schema"""

        # Skip if configured to do so
        if "developer" in self.config:
            if "disable_schema_check" in self.config["developer"]:
                if self.config["developer"]["disable_schema_check"]:
                    return

        this_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
        schema_check = Core(
            source_data=self.config,
            schema_files=[this_dir + "/schema.yaml"],
            extensions=[],
        )

        try:
            pykwalify.init_logging(0)
            schema_check.validate(raise_exception=True)

        except pykwalify.errors.SchemaError:
            # NB logging formatter is not set up yet
            logging.error(
                "Configuration did not validate against schema:\n - {}".format(
                    "\n - ".join(schema_check.validation_errors)
                )
            )
            sys.exit(1)

        except Exception as e:
            logging.exception(e)

    def _interpolate_env_vars(self, config):
        """Resolves environment variables in the config file"""

        for k, v in config.items() if isinstance(config, dict) else enumerate(config):
            if isinstance(v, (list, dict)):
                config[k] = self._interpolate_env_vars(v)
            elif isinstance(v, str):
                config[k] = os.path.expanduser(os.path.expandvars(v))
        return config
