"""Exceptions raised by datachaos, with the exit code each one maps to."""

from __future__ import annotations


class DataChaosError(Exception):
    """A usage, config, IO or tool error."""

    exit_code = 2


class ValidationFailed(DataChaosError):
    """The run completed, but a validation or expectation failed."""

    exit_code = 1
