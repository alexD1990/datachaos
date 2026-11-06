from __future__ import annotations

import pytest

from datachaos.config import DatasetConfig, OutputConfig, RunConfig, validate_config
from datachaos.errors import DataChaosError


def test_defaults_are_valid_and_resolved_in_to_dict() -> None:
    config = RunConfig()
    validate_config(config)
    assert config.to_dict() == {
        "version": 1,
        "seed": 42,
        "dataset": {
            "name": "shop",
            "as_of": "2026-01-01T00:00:00Z",
            "customers": 100,
            "orders": 1000,
            "batches": 1,
            "batch_interval": "1d",
        },
        "output": {"formats": ["csv"], "combined": False, "emit_row_ids": False},
        "scenarios": [],
        "expectations": [],
    }


@pytest.mark.parametrize(
    ("config", "message"),
    [
        (RunConfig(seed=-1), "seed: must be a non-negative integer, got -1"),
        (RunConfig(seed=True), "seed: must be a non-negative integer, got True"),
        (RunConfig(dataset=DatasetConfig(name="nope")), "dataset.name: unknown dataset 'nope'"),
        (
            RunConfig(dataset=DatasetConfig(customers=0)),
            "dataset.customers: must be an integer from 1 to 899999, got 0",
        ),
        (
            RunConfig(dataset=DatasetConfig(customers=900_000)),
            "dataset.customers: must be an integer from 1 to 899999, got 900000",
        ),
        (
            RunConfig(dataset=DatasetConfig(orders=900_000)),
            "dataset.orders: must be an integer from 1 to 899999, got 900000",
        ),
        (
            RunConfig(dataset=DatasetConfig(batches=10_000)),
            "dataset.batches: must be an integer from 1 to 9999, got 10000",
        ),
        (
            RunConfig(dataset=DatasetConfig(as_of="2026-01-01")),
            "dataset.as_of: '2026-01-01' is not a timestamp",
        ),
        (
            RunConfig(dataset=DatasetConfig(batch_interval="2days")),
            "dataset.batch_interval: '2days' is not a duration (use e.g. 2d)",
        ),
        (
            RunConfig(dataset=DatasetConfig(batch_interval="0d")),
            "dataset.batch_interval: batches x batch_interval must be at least 3h, got 1 x 0d",
        ),
        (
            RunConfig(dataset=DatasetConfig(batches=2, batch_interval="89m")),
            "dataset.batch_interval: batches x batch_interval must be at least 3h, got 2 x 89m",
        ),
        (
            RunConfig(dataset=DatasetConfig(as_of="0001-01-01T00:00:00Z")),
            "starts before year 1",
        ),
        (
            RunConfig(dataset=DatasetConfig(batch_interval="99999999999999d")),
            "starts before year 1",
        ),
        (RunConfig(output=OutputConfig(formats=())), "output.formats: must be distinct"),
        (RunConfig(output=OutputConfig(formats=("xml",))), "output.formats: must be distinct"),
        (
            RunConfig(output=OutputConfig(formats=("csv", "csv"))),
            "output.formats: must be distinct",
        ),
    ],
)
def test_invalid_settings_name_their_path(config: RunConfig, message: str) -> None:
    with pytest.raises(DataChaosError) as info:
        validate_config(config)
    assert message in str(info.value)


def test_minimum_window_is_exactly_3h() -> None:
    validate_config(RunConfig(dataset=DatasetConfig(batches=3, batch_interval="1h")))
