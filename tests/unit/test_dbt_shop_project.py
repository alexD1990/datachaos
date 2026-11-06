"""What can be checked of the dbt example without dbt. The real run is in tests/integration."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from datachaos import engine
from datachaos.config import load_config
from datachaos.datasets.shop import CUSTOMERS_SCHEMA, ORDER_EVENTS_SCHEMA, ORDERS_SCHEMA

PROJECT = Path(__file__).resolve().parents[2] / "examples" / "dbt_shop"
CONFIGS = sorted((PROJECT / "configs").glob("*.yml"))
SCHEMAS = {
    "stg_customers": CUSTOMERS_SCHEMA,
    "stg_orders": ORDERS_SCHEMA,
    "stg_order_events": ORDER_EVENTS_SCHEMA,
}


def _yaml(path: Path) -> dict:  # type: ignore[type-arg]
    return yaml.safe_load(path.read_bytes())


def _demo() -> ModuleType:
    spec = importlib.util.spec_from_file_location("run_demo", PROJECT / "run_demo.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_demo"] = module
    spec.loader.exec_module(module)
    return module


def test_there_is_a_baseline_and_all_eight_scenarios() -> None:
    names = {scenario.name for path in CONFIGS for scenario in load_config(path).scenarios}
    assert names == set(engine.SCENARIOS)
    assert "baseline" in {path.stem for path in CONFIGS}


@pytest.mark.parametrize("path", CONFIGS, ids=lambda p: p.stem)
def test_every_config_is_valid_and_says_what_it_expects(path: Path) -> None:
    config = load_config(path)
    assert engine.resolved_run_id(config)
    assert not config.output.emit_row_ids
    if path.stem == "baseline":
        assert config.scenarios == ()
    else:
        assert len(config.scenarios) == 1
        assert {item.scenario for item in config.expectations} == {config.scenarios[0].name}


@pytest.mark.parametrize("path", CONFIGS, ids=lambda p: p.stem)
def test_singular_tests_in_expectations_exist(path: Path) -> None:
    for expectation in load_config(path).expectations:
        for pattern in expectation.failing_tests:
            name = pattern.split(".")[-1].rstrip("*")
            if name.startswith("assert_"):
                assert (PROJECT / "tests" / f"{name}.sql").is_file(), name


def test_sources_read_everything_as_text_and_by_name() -> None:
    tables = _yaml(PROJECT / "models" / "staging" / "sources.yml")["sources"][0]["tables"]
    assert {table["name"] for table in tables} == {"customers", "orders", "order_events"}
    for table in tables:
        location = table["meta"]["external_location"]
        assert "all_varchar=true" in location
        assert "union_by_name=true" in location
        assert f"/{table['name']}/*.csv" in location


def test_staging_is_tables_so_that_a_cast_fails_there() -> None:
    project = _yaml(PROJECT / "dbt_project.yml")
    assert project["models"]["dbt_shop"]["staging"]["+materialized"] == "table"


def test_profile_path_is_relative_by_default() -> None:
    text = (PROJECT / "profiles.yml").read_bytes().decode()
    assert "'dbt_shop.duckdb'" in text


def test_accepted_values_match_the_generator_and_are_strings() -> None:
    """A bare NO in YAML is false, which made this test fail for every Norwegian customer."""
    models = _yaml(PROJECT / "models" / "staging" / "schema.yml")["models"]
    checked = 0
    for model in models:
        schema = SCHEMAS[model["name"]]
        allowed = {column.name: column.allowed_values for column in schema.columns}
        for column in model["columns"]:
            for test in column["data_tests"]:
                if isinstance(test, dict) and "accepted_values" in test:
                    values = test["accepted_values"]["arguments"]["values"]
                    assert all(isinstance(value, str) for value in values), values
                    assert set(values) == set(allowed[column["name"]])
                    checked += 1
    assert checked == 6


def test_the_demo_runs_dbt_per_batch_and_tests_once(tmp_path: Path) -> None:
    demo = _demo()
    calls: list[tuple[str, int]] = []

    def fake_dbt(dbt: str, command: str, work: Path, landing: Path, saved_as: Path) -> None:
        landed = len(list((landing / "orders").glob("*.csv")))
        calls.append((command, landed))
        saved_as.parent.mkdir(parents=True, exist_ok=True)
        results = [{"unique_id": "test.dbt_shop.anything", "status": "pass"}]
        saved_as.write_bytes(json.dumps({"results": results}).encode())

    demo._dbt = fake_dbt
    work = tmp_path / "baseline"
    work.mkdir()
    outcome = demo.run_config(PROJECT / "configs" / "baseline.yml", "dbt", work)
    assert outcome.passed
    assert calls == [("run", 1), ("run", 2), ("run", 3), ("run", 4), ("run", 5), ("test", 5)]


def test_the_demo_fails_a_config_with_an_unexpected_failure(tmp_path: Path) -> None:
    demo = _demo()

    def fake_dbt(dbt: str, command: str, work: Path, landing: Path, saved_as: Path) -> None:
        saved_as.parent.mkdir(parents=True, exist_ok=True)
        results = [
            {
                "unique_id": "test.dbt_shop.assert_raw_orders_unique_per_version",
                "status": "fail" if command == "test" else "skipped",
            },
            {
                "unique_id": "test.dbt_shop.robust",
                "status": "fail" if command == "test" else "pass",
            },
        ]
        saved_as.write_bytes(json.dumps({"results": results}).encode())

    demo._dbt = fake_dbt
    work = tmp_path / "duplicate"
    work.mkdir()
    outcome = demo.run_config(PROJECT / "configs" / "duplicate_business_key.yml", "dbt", work)
    assert not outcome.passed
    assert outcome.check_status == "ok"
    assert outcome.unexpected == ["test.dbt_shop.robust"]


def test_the_demo_passes_unverifiable_lines_on_with_exit_0(tmp_path: Path) -> None:
    demo = _demo()

    def fake_dbt(dbt: str, command: str, work: Path, landing: Path, saved_as: Path) -> None:
        saved_as.parent.mkdir(parents=True, exist_ok=True)
        results = [
            {"unique_id": "test.dbt_shop.assert_raw_orders_unique_per_version", "status": "fail"}
        ]
        saved_as.write_bytes(json.dumps({"results": results}).encode())

    demo._dbt = fake_dbt
    work = tmp_path / "duplicate"
    work.mkdir()
    outcome = demo.run_config(PROJECT / "configs" / "duplicate_business_key.yml", "dbt", work)
    assert outcome.passed
    assert outcome.unverifiable == 10


def test_a_dbt_outside_this_environment_is_not_used(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    demo = _demo()
    searched: list[object] = []

    def which(name: str, path: object = None) -> None:
        searched.append(path)

    monkeypatch.setattr(demo.shutil, "which", which)
    assert demo.main(["--only", "baseline"]) == 2
    assert 'dbt is not installed here; run pip install -e ".[dev,dbt]"' in capsys.readouterr().err
    assert searched == [str(Path(sys.executable).parent)]


def test_an_unknown_config_is_exit_2(capsys: pytest.CaptureFixture[str]) -> None:
    assert _demo().main(["--only", "nope"]) == 2
    assert "unknown config nope" in capsys.readouterr().err
