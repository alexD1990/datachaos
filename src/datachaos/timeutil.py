"""Durations and UTC timestamps. Times are whole seconds since the Unix epoch in memory."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_DURATION = re.compile(r"([0-9]+)(s|m|h|d)")
_TIMESTAMP = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> int:
    """Seconds in a duration such as "90s", "6h" or "2d". Raises ValueError."""
    match = _DURATION.fullmatch(text)
    if match is None:
        raise ValueError(f"{text!r} is not a duration (use e.g. 2d)")
    return int(match.group(1)) * _UNIT_SECONDS[match.group(2)]


def parse_timestamp(text: str) -> int:
    """Epoch seconds from YYYY-MM-DDTHH:MM:SSZ. Raises ValueError."""
    match = _TIMESTAMP.fullmatch(text)
    if match is None:
        raise ValueError(f"{text!r} is not a timestamp (use e.g. 2026-01-01T00:00:00Z)")
    year, month, day, hour, minute, second = (int(part) for part in match.groups())
    try:
        value = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"{text!r} is not a valid timestamp: {exc}") from exc
    return (value - _EPOCH).days * 86400 + (value - _EPOCH).seconds


def format_timestamp(seconds: int) -> str:
    """YYYY-MM-DDTHH:MM:SSZ from epoch seconds. Raises OverflowError outside years 1-9999."""
    value = _EPOCH + timedelta(seconds=seconds)
    return value.replace(tzinfo=None).isoformat(timespec="seconds") + "Z"


def window_start(as_of: int, batches: int, interval: int) -> int:
    return as_of - batches * interval


def batch_index(ingested_at: int, start: int, interval: int) -> int:
    return (ingested_at - start) // interval


def utc_now() -> str:
    """The current time. Only for `manifest.run.created_at`, never for data."""
    return format_timestamp(int((datetime.now(timezone.utc) - _EPOCH).total_seconds()))
