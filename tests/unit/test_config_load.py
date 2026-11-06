from __future__ import annotations

from pathlib import Path

import pytest

from datachaos.config import load_config, parse_param
from datachaos.errors import DataChaosError

GOOD = """\
version: 1
seed: 42
dataset:
  name: shop
  as_of: "2026-01-08T00:00:00Z"
  customers: 200
  orders: 2000
  batches: 7
  batch_interval: 1d
output:
  formats: [csv]
  combined: false
  emit_row_ids: false
scenarios:
  - name: duplicate-business-key
    table: orders
    params: {rate: 0.02}
  - name: late-arriving-data
    table: orders
    params: {rate: 0.05, delay_min: 6h, delay_max: 2d}
expectations:
  - scenario: duplicate-business-key
    failing_tests: ["test.dbt_shop.assert_raw_orders_unique_per_version*"]
"""
MINIMAL = 'version: 1\nseed: 1\ndataset: {as_of: "2026-01-08T00:00:00Z"}\n'


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "chaos.yml"
    path.write_bytes(content.encode())
    return path


def test_the_documented_example_loads(tmp_path: Path) -> None:
    config = load_config(_write(tmp_path, GOOD))
    assert config.seed == 42
    assert config.dataset.batches == 7
    assert config.output.formats == ("csv",)
    assert [scenario.name for scenario in config.scenarios] == [
        "duplicate-business-key",
        "late-arriving-data",
    ]
    assert config.scenarios[1].params == {"rate": 0.05, "delay_min": "6h", "delay_max": "2d"}
    assert config.expectations[0].scenario == "duplicate-business-key"


def test_defaults_apply_to_what_may_be_left_out(tmp_path: Path) -> None:
    config = load_config(_write(tmp_path, MINIMAL))
    assert config.dataset.customers == 100
    assert config.dataset.orders == 1000
    assert config.dataset.batches == 1
    assert config.dataset.batch_interval == "1d"
    assert config.output.formats == ("csv",)
    assert config.scenarios == ()


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("[]", "config: must be a mapping"),
        ("seed: 1\ndataset: {as_of: x}\n", "version: is required"),
        ('version: 1\ndataset: {as_of: "2026-01-08T00:00:00Z"}\n', "seed: is required"),
        ("version: 1\nseed: 1\n", "dataset: is required"),
        ("version: 1\nseed: 1\ndataset: {}\n", "dataset.as_of: is required"),
        ("version: 2\nseed: 1\ndataset: {as_of: x}\n", "version: must be 1, got 2"),
        ("version: true\nseed: 1\ndataset: {as_of: x}\n", "version: must be 1, got True"),
        (MINIMAL + "sed: 2\n", r"sed: unknown key \(did you mean seed\?\)"),
        (
            'version: 1\nseed: 1\ndataset: {as_of: "2026-01-08T00:00:00Z", batchs: 2}\n',
            r"dataset.batchs: unknown key \(did you mean batches\?\)",
        ),
        (
            MINIMAL + "output: {format: [csv]}\n",
            r"output.format: unknown key \(did you mean formats",
        ),
        (MINIMAL + "output: {formats: csv}\n", "output.formats: must be a list"),
        (MINIMAL + "output: {combined: 1}\n", "output.combined: must be true or false"),
        (MINIMAL + "scenarios: {}\n", "scenarios: must be a list"),
        (MINIMAL + "scenarios: [{table: t}]\n", r"scenarios\[0\].name: is required"),
        (MINIMAL + "scenarios: [{name: n}]\n", r"scenarios\[0\].table: is required"),
        (MINIMAL + "scenarios: [{name: 1, table: t}]\n", r"scenarios\[0\].name: must be a string"),
        (MINIMAL + "scenarios: [{name: n, table: 2}]\n", r"scenarios\[0\].table: must be a string"),
        (
            MINIMAL + "scenarios: [{name: n, table: t, param: {}}]\n",
            r"scenarios\[0\].param: unknown key \(did you mean params\?\)",
        ),
        (MINIMAL + "scenarios: [{name: n, table: t, params: 1}]\n", r"scenarios\[0\].params: must"),
        (MINIMAL + "scenarios: [{name: n, table: t, params: {1: 2}}]\n", "names must be strings"),
        (MINIMAL + "expectations: [{scenario: n}]\n", r"expectations\[0\].failing_tests: is req"),
        (
            MINIMAL + "expectations: [{scenario: n, failing_tests: []}]\n",
            r"expectations\[0\].failing_tests: must be a non-empty",
        ),
        (
            MINIMAL + "expectations: [{scenario: n, failing_tests: [x]}]\n",
            r"expectations\[0\].scenario: 'n' is not a scenario in this config",
        ),
        (
            MINIMAL
            + "scenarios: [{name: dup, table: t}]\n"
            + "expectations: [{scenario: dip, failing_tests: [x]}]\n",
            r"\(did you mean dup\?\)",
        ),
        (
            "version: 1\nseed: 1\ndataset: {as_of: 2026-01-08T00:00:00Z}\n",
            "dataset.as_of: must be a quoted string",
        ),
        (
            'version: 1\nseed: 1\ndataset: {as_of: "2026-01-08T00:00:00Z", batch_interval: 5}\n',
            "dataset.batch_interval: must be a string",
        ),
        (
            'version: 1\nseed: 1\ndataset: {as_of: "2026-01-08T00:00:00Z", '
            "batch_interval: 2days}\n",
            r"dataset.batch_interval: '2days' is not a duration \(use e.g. 2d\)",
        ),
        ('version: 1\nseed: x\ndataset: {as_of: "2026-01-08T00:00:00Z"}\n', "seed: must be"),
        (
            'version: 1\nseed: 1\ndataset: {as_of: "2026-01-08T00:00:00Z", orders: many}\n',
            "dataset.orders: must be an integer",
        ),
        (MINIMAL + "output: {formats: [xml]}\n", "output.formats: must be distinct"),
        ("version: 1\nversion: 1\n", "line 2: duplicate key 'version'"),
    ],
)
def test_each_kind_of_error_names_its_path(tmp_path: Path, content: str, message: str) -> None:
    with pytest.raises(DataChaosError, match=message):
        load_config(_write(tmp_path, content))


def test_invalid_yaml_and_missing_file_are_one_line_errors(tmp_path: Path) -> None:
    with pytest.raises(DataChaosError, match="invalid YAML") as info:
        load_config(_write(tmp_path, "a: [\nb"))
    assert "\n" not in str(info.value)
    with pytest.raises(DataChaosError, match="cannot read config"):
        load_config(tmp_path / "missing.yml")


def test_python_tags_are_not_executed(tmp_path: Path) -> None:
    content = "version: !!python/object/apply:os.getcwd []\n"
    with pytest.raises(DataChaosError, match="invalid YAML"):
        load_config(_write(tmp_path, content))


@pytest.mark.parametrize(
    ("text", "name", "value"),
    [
        ("rate=0.02", "rate", 0.02),
        ("count=3", "count", 3),
        ("allow_key=true", "allow_key", True),
        ("values=[a,b]", "values", ["a", "b"]),
        ('delay="3d"', "delay", "3d"),
        ("delay=3d", "delay", "3d"),
        ("note=a=b", "note", "a=b"),
    ],
)
def test_parse_param(text: str, name: str, value: object) -> None:
    assert parse_param(text) == (name, value)


@pytest.mark.parametrize("text", ["rate", "=1", "x=[a"])
def test_parse_param_rejects(text: str) -> None:
    with pytest.raises(DataChaosError, match="--param"):
        parse_param(text)
