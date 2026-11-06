from __future__ import annotations

import json
from pathlib import Path

import pytest
from helpers import config

from datachaos import __version__, cli, engine
from datachaos.config import ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.report import build_report


def _run(tmp_path: Path, *scenarios: ScenarioConfig) -> Path:
    out = tmp_path / "out"
    engine.run(config(*scenarios), out)
    return out


NULLS = ScenarioConfig("null-critical-column", "orders", {"column": "amount", "count": 8})
DROP = ScenarioConfig(
    "schema-drift", "customers", {"operation": "drop_column", "column": "is_active"}
)
ORPHANS = ScenarioConfig("orphaned-foreign-key", "orders", {"mode": "deleted_parent", "count": 1})


def test_a_scenario_has_counts_per_action_and_five_example_keys(tmp_path: Path) -> None:
    report = build_report(_run(tmp_path, NULLS))
    scenario = report["scenarios"][0]  # type: ignore[index]
    assert scenario["name"] == "null-critical-column"
    assert scenario["actions"] == {"set_value": 8}
    assert len(scenario["example_keys"]) == 5
    assert all(set(key) == {"order_id"} for key in scenario["example_keys"])
    assert scenario["stats"]["rows_selected"] == 8
    assert scenario["params"]["column"] == "amount"


def test_example_keys_are_distinct(tmp_path: Path) -> None:
    update = ScenarioConfig("historical-update", "orders", {"older_than": "1d", "count": 7})
    scenario = build_report(_run(tmp_path, update))["scenarios"][0]  # type: ignore[index]
    keys = [key["order_id"] for key in scenario["example_keys"]]
    assert len(keys) == len(set(keys)) == 5
    assert scenario["actions"] == {"insert_row": 7, "set_value": 14}


def test_several_actions_are_counted(tmp_path: Path) -> None:
    scenario = build_report(_run(tmp_path, ORPHANS))["scenarios"][0]  # type: ignore[index]
    assert set(scenario["actions"]) == {"delete_row", "orphan_row"}
    assert scenario["actions"]["delete_row"] == 1


def test_a_schema_change_is_described_and_has_no_example_keys(tmp_path: Path) -> None:
    scenario = build_report(_run(tmp_path, DROP))["scenarios"][0]  # type: ignore[index]
    assert scenario["example_keys"] == []
    assert scenario["schema_changes"] == [
        {"column": "is_active", "before": "is_active", "after": None, "from_batch": 2}
    ]


def test_the_lines_of_scenarios_are_kept_apart(tmp_path: Path) -> None:
    report = build_report(_run(tmp_path, NULLS, DROP))
    assert [s["actions"] for s in report["scenarios"]] == [  # type: ignore[attr-defined]
        {"set_value": 8},
        {"schema_change": 1},
    ]
    assert report["ledger_lines"] == 9


def test_a_baseline_has_no_scenarios(tmp_path: Path) -> None:
    report = build_report(_run(tmp_path))
    assert report["scenarios"] == []
    assert report["ledger_lines"] == 0


def test_report_checks_nothing(tmp_path: Path) -> None:
    out = _run(tmp_path, NULLS)
    path = out / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(b"not a csv")
    assert build_report(out)["ledger_lines"] == 8


def test_not_datachaos_output_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(DataChaosError, match="has no chaos-manifest.json"):
        build_report(tmp_path)


def test_a_broken_ledger_is_an_error(tmp_path: Path) -> None:
    out = _run(tmp_path, NULLS)
    (out / "chaos-ledger.jsonl").write_bytes(b"{nope\n")
    with pytest.raises(DataChaosError, match="cannot read the ledger"):
        build_report(out)


def test_cli_text(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = _run(tmp_path, NULLS, DROP)
    assert cli.main(["report", str(out)]) == 0
    text = capsys.readouterr().out
    assert "  orders: 100 -> 100 rows\n" in text
    assert "scenario 0: null-critical-column on orders (version 1)\n" in text
    assert "  ledger: set_value 8\n" in text
    assert "  example keys: O" in text
    assert "  is_active: is_active -> None from batch 2\n" in text
    assert "Not checked; use validate" in text


def test_cli_json_is_only_json_with_versions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(tmp_path, NULLS)
    assert cli.main(["report", str(out), "--output", "json"]) == 0
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert captured.err == ""
    assert document["schema_version"] == "1.0"
    assert document["tool_version"] == __version__
    assert document["scenarios"][0]["actions"] == {"set_value": 8}
    assert document["warnings"]


def test_cli_not_datachaos_output_is_exit_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["report", str(tmp_path)]) == 2
    assert capsys.readouterr().err.startswith("datachaos: error:")


def test_help_says_it_checks_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli.main(["report", "--help"])
    assert "Checks nothing" in capsys.readouterr().out
