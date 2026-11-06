"""Exceptions raised by datachaos, with the exit code each one maps to."""

from __future__ import annotations


class DataChaosError(Exception):
    """A usage, config, IO or tool error."""

    exit_code = 2


class ValidationFailed(DataChaosError):
    """The run completed, but a validation or expectation failed."""

    exit_code = 1


class FieldError(DataChaosError):
    """An invalid setting, named by its path relative to the section that holds it."""

    def __init__(self, field: str, message: str) -> None:
        super().__init__(f"{field}: {message}")
        self.field = field
        self.message = message
