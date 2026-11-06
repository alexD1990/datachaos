"""The resolved configuration of a run, and its validation."""

from __future__ import annotations

import difflib
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

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
class ScenarioConfig:
    name: str
    table: str
    params: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "table": self.table, "params": dict(self.params)}


@dataclass(frozen=True)
class ExpectationConfig:
    scenario: str
    failing_tests: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {"scenario": self.scenario, "failing_tests": list(self.failing_tests)}


@dataclass(frozen=True)
class RunConfig:
    seed: int = DEFAULT_SEED
    dataset: DatasetConfig = DatasetConfig()
    output: OutputConfig = OutputConfig()
    scenarios: tuple[ScenarioConfig, ...] = ()
    expectations: tuple[ExpectationConfig, ...] = ()

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
            "scenarios": [scenario.to_dict() for scenario in self.scenarios],
            "expectations": [expectation.to_dict() for expectation in self.expectations],
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
    names = {scenario.name for scenario in config.scenarios}
    for index, expectation in enumerate(config.expectations):
        if expectation.scenario not in names:
            raise DataChaosError(
                f"expectations[{index}].scenario: {expectation.scenario!r} is not a scenario "
                f"in this config{_suggestion(expectation.scenario, names)}"
            )


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_range(path: str, value: int, maximum: int) -> None:
    if not _is_int(value) or not 1 <= value <= maximum:
        raise DataChaosError(f"{path}: must be an integer from 1 to {maximum}, got {value!r}")


def _check_window(dataset: DatasetConfig) -> None:
    if not isinstance(dataset.as_of, str):
        raise DataChaosError(
            f"dataset.as_of: must be a quoted string such as {DEFAULT_AS_OF!r}, "
            f"got {dataset.as_of!r}"
        )
    if not isinstance(dataset.batch_interval, str):
        raise DataChaosError(
            f"dataset.batch_interval: must be a string such as '1d', got {dataset.batch_interval!r}"
        )
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


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that refuses duplicate keys, which YAML would otherwise resolve silently."""


def _construct_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode) -> dict[Hashable, Any]:
    seen = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in seen:
            line = key_node.start_mark.line + 1
            raise DataChaosError(f"line {line}: duplicate key {key!r}")
        seen.add(key)
    return loader.construct_mapping(node, deep=True)


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def load_config(path: Path) -> RunConfig:
    """Reads and validates a config file. Raises DataChaosError naming the invalid setting."""
    try:
        with open(path, encoding="utf-8", newline="") as file:
            data = yaml.load(file, Loader=_StrictLoader)
    except OSError as exc:
        raise DataChaosError(f"cannot read config {path}: {exc}") from exc
    except DataChaosError as exc:
        raise DataChaosError(f"{path}: {exc}") from exc
    except (yaml.YAMLError, ValueError) as exc:
        raise DataChaosError(f"invalid YAML in {path}: {' '.join(str(exc).split())}") from exc
    config = parse_config(data)
    validate_config(config)
    return config


def parse_config(data: object) -> RunConfig:
    top = _mapping(
        data,
        "config",
        ("version", "seed", "dataset", "output", "scenarios", "expectations"),
        ("version", "seed", "dataset"),
    )
    if top["version"] != 1 or isinstance(top["version"], bool):
        raise DataChaosError(f"version: must be 1, got {top['version']!r}")
    return RunConfig(
        seed=top["seed"],
        dataset=_parse_dataset(top["dataset"]),
        output=_parse_output(top.get("output", {})),
        scenarios=tuple(
            _parse_scenario(item, f"scenarios[{index}]")
            for index, item in enumerate(_list(top.get("scenarios", []), "scenarios"))
        ),
        expectations=tuple(
            _parse_expectation(item, f"expectations[{index}]")
            for index, item in enumerate(_list(top.get("expectations", []), "expectations"))
        ),
    )


def parse_param(text: str) -> tuple[str, object]:
    """Splits a `--param key=value`. The value is YAML: 0.02, true, [a,b] or "3d"."""
    name, separator, value = text.partition("=")
    if not separator or not name:
        raise DataChaosError(f"--param: {text!r} is not key=value")
    try:
        return name, yaml.safe_load(value)
    except yaml.YAMLError as exc:
        raise DataChaosError(f"--param {name}: invalid value {value!r}") from exc


def parse_params(texts: list[str]) -> dict[str, object]:
    params: dict[str, object] = {}
    for text in texts:
        name, value = parse_param(text)
        if name in params:
            raise DataChaosError(f"--param: {name} is given more than once")
        params[name] = value
    return params


def _parse_dataset(data: object) -> DatasetConfig:
    section = _mapping(
        data,
        "dataset",
        ("name", "as_of", "customers", "orders", "batches", "batch_interval"),
        ("as_of",),
    )
    return DatasetConfig(
        name=section.get("name", "shop"),
        as_of=section["as_of"],
        customers=section.get("customers", DEFAULT_CUSTOMERS),
        orders=section.get("orders", DEFAULT_ORDERS),
        batches=section.get("batches", DEFAULT_BATCHES),
        batch_interval=section.get("batch_interval", DEFAULT_BATCH_INTERVAL),
    )


def _parse_output(data: object) -> OutputConfig:
    section = _mapping(data, "output", ("formats", "combined", "emit_row_ids"), ())
    for name in ("combined", "emit_row_ids"):
        if not isinstance(section.get(name, False), bool):
            raise DataChaosError(f"output.{name}: must be true or false, got {section[name]!r}")
    return OutputConfig(
        formats=tuple(_list(section.get("formats", ["csv"]), "output.formats")),
        combined=section.get("combined", False),
        emit_row_ids=section.get("emit_row_ids", False),
    )


def _parse_scenario(data: object, path: str) -> ScenarioConfig:
    section = _mapping(data, path, ("name", "table", "params"), ("name", "table"))
    for name in ("name", "table"):
        if not isinstance(section[name], str):
            raise DataChaosError(f"{path}.{name}: must be a string, got {section[name]!r}")
    params = section.get("params", {})
    if not isinstance(params, dict):
        raise DataChaosError(f"{path}.params: must be a mapping, got {params!r}")
    for key in params:
        if not isinstance(key, str):
            raise DataChaosError(f"{path}.params: names must be strings, got {key!r}")
    return ScenarioConfig(section["name"], section["table"], params)


def _parse_expectation(data: object, path: str) -> ExpectationConfig:
    section = _mapping(data, path, ("scenario", "failing_tests"), ("scenario", "failing_tests"))
    if not isinstance(section["scenario"], str):
        raise DataChaosError(f"{path}.scenario: must be a string, got {section['scenario']!r}")
    tests = _list(section["failing_tests"], f"{path}.failing_tests")
    if not tests or not all(isinstance(test, str) and test for test in tests):
        raise DataChaosError(f"{path}.failing_tests: must be a non-empty list of strings")
    return ExpectationConfig(section["scenario"], tuple(tests))


def _mapping(
    data: object, path: str, allowed: tuple[str, ...], required: tuple[str, ...]
) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise DataChaosError(f"{path}: must be a mapping, got {data!r}")
    section: dict[str, Any] = data
    prefix = "" if path == "config" else f"{path}."
    for key in sorted(section, key=str):
        if key not in allowed:
            hint = _suggestion(str(key), allowed)
            raise DataChaosError(f"{prefix}{key}: unknown key{hint}")
    for key in required:
        if key not in section:
            raise DataChaosError(f"{prefix}{key}: is required")
    return section


def _list(data: object, path: str) -> list[Any]:
    if not isinstance(data, list):
        raise DataChaosError(f"{path}: must be a list, got {data!r}")
    return data


def _suggestion(name: str, options: Any) -> str:
    matches = difflib.get_close_matches(name, sorted(options), n=1)
    return f" (did you mean {matches[0]}?)" if matches else ""
