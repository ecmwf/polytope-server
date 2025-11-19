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

import sys

from ..common.config import ConfigParser


class Config:
    def __init__(self, allow_empty: bool | None = None):
        """
        Lightweight wrapper around ConfigParser for telemetry.

        - In normal runtime (no pytest), any config/schema error should abort
        - In tests (pytest), we allow an empty config so telemetry modules
          can be imported without requiring a real config file.
        """
        # Auto-detect test environment if not explicitly specified
        if allow_empty is None:
            allow_empty = "pytest" in sys.modules

        self._allow_empty = allow_empty

        try:
            self.config = ConfigParser().read()
        except SystemExit:
            # ConfigParser uses sys.exit(1) on schema/validation failure.
            if self._allow_empty:
                # In tests: just use an empty config.
                self.config = {}
            else:
                raise
        except Exception:
            # Any other error (e.g. no config files) – same policy.
            if self._allow_empty:
                self.config = {}
            else:
                raise

    def get(self, section: str, default=None):
        """Retrieve a section from the config or return a default value."""
        return (self.config or {}).get(section, default)


# Global config instance
config = Config()
