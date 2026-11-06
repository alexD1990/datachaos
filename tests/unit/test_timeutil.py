from __future__ import annotations

import pytest

from datachaos.timeutil import (
    batch_index,
    format_timestamp,
    parse_duration,
    parse_timestamp,
    utc_now,
    window_start,
)


@pytest.mark.parametrize(
    ("text", "seconds"), [("90s", 90), ("2m", 120), ("6h", 21600), ("2d", 172800), ("0d", 0)]
)
def test_parse_duration(text: str, seconds: int) -> None:
    assert parse_duration(text) == seconds


@pytest.mark.parametrize("text", ["2days", "", "1", "h", "-1d", "1.5h", "1d\n", "١d"])
def test_parse_duration_rejects(text: str) -> None:
    with pytest.raises(ValueError, match="is not a duration"):
        parse_duration(text)


def test_timestamp_round_trip() -> None:
    assert parse_timestamp("1970-01-01T00:00:00Z") == 0
    assert parse_timestamp("2026-01-01T00:00:00Z") == 1767225600
    assert format_timestamp(1767225600) == "2026-01-01T00:00:00Z"
    assert format_timestamp(-1) == "1969-12-31T23:59:59Z"
    assert format_timestamp(parse_timestamp("0987-06-05T04:03:02Z")) == "0987-06-05T04:03:02Z"


@pytest.mark.parametrize(
    "text",
    [
        "2026-01-01",
        "2026-01-01T00:00:00",
        "2026-01-01T00:00:00+00:00",
        "2026-1-1T00:00:00Z",
        "2026-01-01T00:00:00Z\n",
    ],
)
def test_parse_timestamp_rejects_other_formats(text: str) -> None:
    with pytest.raises(ValueError, match="is not a timestamp"):
        parse_timestamp(text)


def test_parse_timestamp_rejects_impossible_dates() -> None:
    with pytest.raises(ValueError, match="is not a valid timestamp"):
        parse_timestamp("2026-02-30T00:00:00Z")


def test_batch_boundaries_are_half_open() -> None:
    start = window_start(1000, 2, 100)
    assert start == 800
    assert batch_index(800, start, 100) == 0
    assert batch_index(899, start, 100) == 0
    assert batch_index(900, start, 100) == 1
    assert batch_index(999, start, 100) == 1
    assert batch_index(1000, start, 100) == 2


def test_utc_now_is_a_canonical_timestamp() -> None:
    now = utc_now()
    assert format_timestamp(parse_timestamp(now)) == now
