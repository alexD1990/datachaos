"""Acceptance for phase 3: all seven scenarios together, checked against the files on disk."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from datachaos import engine
from datachaos.config import load_config, parse_config
from datachaos.io.reader import read_csv, read_json, read_schema

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "tests" / "golden" / "all_seven_scenarios" / "chaos.yml"
RID = "_chaos_rid"


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.fixture(scope="module")
def out(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("run") / "out"
    engine.run(load_config(CONFIG), path)
    return path


def _ledger(out: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (out / "chaos-ledger.jsonl").read_bytes().splitlines()]


def _rows(out: Path, table: str) -> dict[str, tuple[int, dict[str, Any]]]:
    """Every row of a table by rid, with the batch whose file it is in."""
    rows = {}
    for path in sorted((out / "data" / table).glob("batch_*.csv")):
        batch = int(path.stem.split("_")[1])
        schema = read_schema(out / "schemas" / table / f"{path.stem}.json")
        for row in read_csv(path, schema):
            rows[str(row[RID])] = (batch, row)
    return rows


def test_all_seven_ran_and_the_manifest_validates(out: Path) -> None:
    manifest = read_json(out / "chaos-manifest.json")
    schema = read_json(REPO / "schema" / "chaos-manifest-1.0.schema.json")
    jsonschema.Draft202012Validator(schema).validate(manifest)
    assert [scenario["name"] for scenario in manifest["scenarios"]] == [
        "duplicate-business-key",
        "null-critical-column",
        "invalid-enum",
        "orphaned-foreign-key",
        "late-arriving-data",
        "out-of-order-events",
        "historical-update",
    ]
    assert all(scenario["ledger_entries"] > 0 for scenario in manifest["scenarios"])


def test_every_ledger_line_is_visible_in_the_files(out: Path) -> None:
    rows = {table: _rows(out, table) for table in ("customers", "orders", "order_events")}
    actions = set()
    for entry in _ledger(out):
        actions.add(entry["action"])
        table = rows[entry["table"]]
        if entry["action"] == "delete_row":
            assert entry["rid"] not in table
            continue
        batch, row = table[entry["rid"]]
        assert batch == entry["batch"], entry
        if entry["action"] == "set_value" or entry["action"] == "orphan_row":
            assert row[entry["column"]] == entry["after"], entry
        elif entry["action"] == "move_batch":
            assert batch == entry["after"], entry
            assert entry["before"] != entry["after"]
    assert actions == {"insert_row", "set_value", "delete_row", "orphan_row", "move_batch"}


def test_orphans_have_no_parent_on_disk(out: Path) -> None:
    customers = {row["customer_id"] for _, row in _rows(out, "customers").values()}
    orders = _rows(out, "orders")
    orphans = [entry for entry in _ledger(out) if entry["action"] == "orphan_row"]
    assert orphans
    for entry in orphans:
        assert orders[entry["rid"]][1]["customer_id"] not in customers


def test_no_two_scenarios_touch_the_same_key_or_group(out: Path) -> None:
    owners: dict[tuple[str, str], set[int]] = defaultdict(set)
    for entry in _ledger(out):
        for value in entry["key"].values():
            owners[(entry["table"], value)].add(entry["scenario_index"])
    assert all(len(scenarios) == 1 for scenarios in owners.values())


def test_rows_in_a_touched_group_are_left_to_that_scenario(out: Path) -> None:
    events = _rows(out, "order_events")
    entries = _ledger(out)
    groups = {
        entry["group"]["order_id"]: entry["scenario_index"]
        for entry in entries
        if entry.get("group")
    }
    assert groups
    for entry in entries:
        if entry["table"] == "order_events" and not entry.get("group"):
            assert events[entry["rid"]][1]["order_id"] not in groups


def test_out_of_order_groups_are_out_of_order_on_disk(out: Path) -> None:
    by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for _, row in _rows(out, "order_events").values():
        by_order[str(row["order_id"])].append(row)
    groups = {entry["group"]["order_id"] for entry in _ledger(out) if entry.get("group")}
    for order in groups:
        rows = sorted(by_order[order], key=lambda row: str(row["event_time"]))
        assert rows[-2]["ingested_at"] > rows[-1]["ingested_at"]


def test_the_config_in_the_manifest_reproduces_the_run_byte_for_byte(tmp_path: Path) -> None:
    moment = "2000-01-01T00:00:00Z"
    engine.run(load_config(CONFIG), tmp_path / "a", created_at=moment)
    resolved = read_json(tmp_path / "a" / "chaos-manifest.json")["run"]["config"]
    engine.run(parse_config(resolved), tmp_path / "b", created_at=moment)
    assert _tree(tmp_path / "a") == _tree(tmp_path / "b")


def test_two_processes_give_identical_files(tmp_path: Path) -> None:
    for name, hash_seed in (("a", "1"), ("b", "2")):
        subprocess.run(
            [
                sys.executable,
                "-m",
                "datachaos",
                "run",
                "--config",
                str(CONFIG),
                "--out",
                str(tmp_path / name),
            ],
            check=True,
            capture_output=True,
            env={**os.environ, "PYTHONHASHSEED": hash_seed},
        )
    a = _tree(tmp_path / "a")
    b = _tree(tmp_path / "b")
    first = json.loads(a.pop("chaos-manifest.json"))
    second = json.loads(b.pop("chaos-manifest.json"))
    assert a == b
    assert first["run"].pop("created_at")
    assert second["run"].pop("created_at")
    assert first == second
