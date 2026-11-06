"""Acceptance for phase 2: a config with all three scenarios gives a reproducible result."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import jsonschema
import pytest

import datachaos
from datachaos import engine
from datachaos.config import load_config, parse_config
from datachaos.errors import DataChaosError
from datachaos.io.reader import read_json

REPO = Path(__file__).resolve().parents[2]
CONFIG = REPO / "tests" / "golden" / "all_scenarios_small" / "chaos.yml"


def _run(out: Path, config: Path, *, hash_seed: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "datachaos", "run", "--config", str(config), "--out", str(out)],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONHASHSEED": hash_seed},
    )


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _with_seed(tmp_path: Path, seed: int) -> Path:
    content = CONFIG.read_bytes().replace(b"seed: 20260105", f"seed: {seed}".encode())
    path = tmp_path / f"seed-{seed}.yml"
    path.write_bytes(content)
    return path


def test_same_config_gives_identical_files_across_processes(tmp_path: Path) -> None:
    _run(tmp_path / "a", CONFIG, hash_seed="1")
    _run(tmp_path / "b", CONFIG, hash_seed="2")
    a = _tree(tmp_path / "a")
    b = _tree(tmp_path / "b")
    first = json.loads(a.pop("chaos-manifest.json"))
    second = json.loads(b.pop("chaos-manifest.json"))
    assert a == b
    assert first["run"].pop("created_at")
    assert second["run"].pop("created_at")
    assert first == second


def test_another_seed_gives_another_result(tmp_path: Path) -> None:
    _run(tmp_path / "a", CONFIG, hash_seed="1")
    _run(tmp_path / "b", _with_seed(tmp_path, 7), hash_seed="1")
    assert (
        read_json(tmp_path / "a" / "chaos-manifest.json")["content_sha256"]
        != read_json(tmp_path / "b" / "chaos-manifest.json")["content_sha256"]
    )


def test_the_config_in_the_manifest_reproduces_the_run_byte_for_byte(tmp_path: Path) -> None:
    moment = "2000-01-01T00:00:00Z"
    engine.run(load_config(CONFIG), tmp_path / "a", created_at=moment)
    resolved = read_json(tmp_path / "a" / "chaos-manifest.json")["run"]["config"]
    engine.run(parse_config(resolved), tmp_path / "b", created_at=moment)
    assert _tree(tmp_path / "a") == _tree(tmp_path / "b")


def test_the_manifest_validates_and_records_all_scenarios(tmp_path: Path) -> None:
    _run(tmp_path / "out", CONFIG, hash_seed="1")
    manifest = read_json(tmp_path / "out" / "chaos-manifest.json")
    schema = read_json(REPO / "schema" / "chaos-manifest-1.0.schema.json")
    jsonschema.Draft202012Validator(schema).validate(manifest)
    assert [scenario["name"] for scenario in manifest["scenarios"]] == [
        "duplicate-business-key",
        "null-critical-column",
        "invalid-enum",
    ]
    assert manifest["warnings"]
    assert manifest["dataset"]["tables"]["orders"] == {"rows_baseline": 50, "rows_final": 55}


def test_every_ledger_line_is_visible_in_the_data_files(tmp_path: Path) -> None:
    out = tmp_path / "out"
    config = load_config(CONFIG)
    engine.run(config, out)
    rows_by_key: dict[object, list[dict[str, object]]] = {}
    for path in sorted((out / "data" / "orders").glob("*.jsonl")):
        for line in path.read_bytes().splitlines():
            row = json.loads(line)
            rows_by_key.setdefault(row["order_id"], []).append(row)
    lines = (out / "chaos-ledger.jsonl").read_bytes().splitlines()
    assert lines
    for line in lines:
        entry = json.loads(line)
        rows = rows_by_key[entry["key"]["order_id"]]
        if entry["action"] == "insert_row":
            assert len(rows) == 2
        else:
            assert entry["after"] in [row[entry["column"]] for row in rows]


def test_scenarios_do_not_overlap_by_default(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.run(load_config(CONFIG), out)
    keys = [
        json.loads(line)["key"]["order_id"]
        for line in (out / "chaos-ledger.jsonl").read_bytes().splitlines()
    ]
    assert len(keys) == len(set(keys))


def test_docs_are_up_to_date() -> None:
    expected = engine.scenarios_markdown().encode()
    assert (REPO / "docs" / "scenarios.md").read_bytes() == expected, (
        "docs/scenarios.md is out of date; run python -m datachaos._docs"
    )


def test_public_run(tmp_path: Path) -> None:
    manifest = datachaos.run(CONFIG, tmp_path / "out")
    assert [record["name"] for record in manifest.scenarios] == [
        "duplicate-business-key",
        "null-critical-column",
        "invalid-enum",
    ]
    assert not manifest.run.created_at.startswith("2000")
    with pytest.raises(DataChaosError, match="is not empty"):
        datachaos.run(CONFIG, tmp_path / "out")
    assert datachaos.run(CONFIG, tmp_path / "out", force=True).run.run_id == manifest.run.run_id
