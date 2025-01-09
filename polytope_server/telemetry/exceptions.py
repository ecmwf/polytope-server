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
