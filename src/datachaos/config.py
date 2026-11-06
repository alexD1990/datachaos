"""The resolved configuration of a run, and its validation."""

from __future__ import annotations

from dataclasses import dataclass

from datachaos.errors import DataChaosError
from datachaos.timeutil import format_timestamp, parse_duration, parse_timestamp, window_start

DEFAULT_AS_OF = "2026-01-01T00:00:00Z"
DEFAULT_SEED = 42
DEFAULT_CUSTOMERS = 100
DEFAULT_ORDERS = 1000
DEFAULT_BATCHES = 1
DEFAULT_BATCH_INTERVAL = "1d"

# IDs have six digits, and the 9xxxxx series is reserved for references that do not exist.
MAX_ROWS = 899_999
MAX_BATCHES = 9_999
MIN_WINDOW_SECONDS = 3 * 3600
FORMATS = ("csv", "jsonl")


@dataclass(frozen=True)
class DatasetConfig:
    name: str = "shop"
    as_of: str = DEFAULT_AS_OF
    customers: int = DEFAULT_CUSTOMERS
    orders: int = DEFAULT_ORDERS
    batches: int = DEFAULT_BATCHES
    batch_interval: str = DEFAULT_BATCH_INTERVAL


@dataclass(frozen=True)
class OutputConfig:
    formats: tuple[str, ...] = ("csv",)
    combined: bool = False
    emit_row_ids: bool = False


@dataclass(frozen=True)
class RunConfig:
    seed: int = DEFAULT_SEED
    dataset: DatasetConfig = DatasetConfig()
    output: OutputConfig = OutputConfig()

    def to_dict(self) -> dict[str, object]:
        return {
            "version": 1,
            "seed": self.seed,
            "dataset": {
                "name": self.dataset.name,
                "as_of": self.dataset.as_of,
                "customers": self.dataset.customers,
                "orders": self.dataset.orders,
                "batches": self.dataset.batches,
                "batch_interval": self.dataset.batch_interval,
            },
            "output": {
                "formats": list(self.output.formats),
                "combined": self.output.combined,
                "emit_row_ids": self.output.emit_row_ids,
            },
            "scenarios": [],
            "expectations": [],
        }


def validate_config(config: RunConfig) -> None:
    """Raises DataChaosError with the path of the first invalid setting."""
    if not _is_int(config.seed) or config.seed < 0:
        raise DataChaosError(f"seed: must be a non-negative integer, got {config.seed!r}")
    dataset = config.dataset
    if dataset.name != "shop":
        raise DataChaosError(f"dataset.name: unknown dataset {dataset.name!r} (available: shop)")
    _check_range("dataset.customers", dataset.customers, MAX_ROWS)
    _check_range("dataset.orders", dataset.orders, MAX_ROWS)
    _check_range("dataset.batches", dataset.batches, MAX_BATCHES)
    _check_window(dataset)
    formats = config.output.formats
    if not formats or len(set(formats)) != len(formats) or not set(formats) <= set(FORMATS):
        raise DataChaosError(
            f"output.formats: must be distinct values from {list(FORMATS)}, got {list(formats)}"
        )


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_range(path: str, value: int, maximum: int) -> None:
    if not _is_int(value) or not 1 <= value <= maximum:
        raise DataChaosError(f"{path}: must be an integer from 1 to {maximum}, got {value!r}")


def _check_window(dataset: DatasetConfig) -> None:
    try:
        as_of = parse_timestamp(dataset.as_of)
    except ValueError as exc:
        raise DataChaosError(f"dataset.as_of: {exc}") from exc
    try:
        interval = parse_duration(dataset.batch_interval)
    except ValueError as exc:
        raise DataChaosError(f"dataset.batch_interval: {exc}") from exc
    if dataset.batches * interval < MIN_WINDOW_SECONDS:
        raise DataChaosError(
            f"dataset.batch_interval: batches x batch_interval must be at least 3h, got "
            f"{dataset.batches} x {dataset.batch_interval}"
        )
    try:
        format_timestamp(window_start(as_of, dataset.batches, interval))
    except OverflowError as exc:
        raise DataChaosError(
            f"dataset.batch_interval: the window of {dataset.batches} x "
            f"{dataset.batch_interval} before {dataset.as_of} starts before year 1"
        ) from exc
