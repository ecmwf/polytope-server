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


class TelemetryError(Exception):
    """Base class for telemetry-related errors."""

    pass


class TelemetryUsageDisabled(TelemetryError):
    """Raised when telemetry usage is disabled."""

    pass


class RequestFetchError(TelemetryError):
    """Raised when fetching requests fails."""

    pass


class MetricCalculationError(TelemetryError):
    """Raised when metric calculation fails."""

    pass


class OutputFormatError(TelemetryError):
    """Raised when an invalid output format is requested."""

    pass


class TelemetryConfigError(TelemetryError):
    """Raised when there is an issue with the telemetry configuration."""

    pass


class TelemetryCacheError(TelemetryError):
    """Raised when there is an issue with caching telemetry data."""

    pass


class TelemetryDataError(TelemetryError):
    """Raised when there is an issue with telemetry data."""

    pass
