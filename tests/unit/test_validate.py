from __future__ import annotations

import json
from pathlib import Path

import pytest
from helpers import config

from datachaos import __version__, cli, engine
from datachaos.config import DatasetConfig, OutputConfig, RunConfig, ScenarioConfig
from datachaos.errors import DataChaosError
from datachaos.io.output_dir import sha256_file
from datachaos.model.manifest import FileEntry, compute_content_sha256
from datachaos.validate import is_safe_path, validate_dir

SMALL = DatasetConfig(as_of="2026-01-08T00:00:00Z", customers=20, orders=100, batches=3)


def _run(
    tmp_path: Path, *scenarios: ScenarioConfig, row_ids: bool = False, **output: object
) -> Path:
    out = tmp_path / "out"
    settings = OutputConfig(emit_row_ids=row_ids, **output)  # type: ignore[arg-type]
    engine.run(RunConfig(seed=5, dataset=SMALL, output=settings, scenarios=scenarios), out)
    return out


def _null(**params: object) -> ScenarioConfig:
    return ScenarioConfig("null-critical-column", "orders", params)


def _enum(**params: object) -> ScenarioConfig:
    return ScenarioConfig("invalid-enum", "orders", params)


def _resign(out: Path) -> None:
    """Makes the hashes match after a deliberate change, so that only the content can fail."""
    path = out / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    for entry in manifest["files"]:
        entry["sha256"] = sha256_file(out / entry["path"])
    manifest["content_sha256"] = compute_content_sha256(
        FileEntry(entry["path"], entry["sha256"]) for entry in manifest["files"]
    )
    path.write_bytes(json.dumps(manifest).encode())


def _ledger(out: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in (out / "chaos-ledger.jsonl").read_bytes().splitlines()]


def _write_ledger(out: Path, lines: list[dict[str, object]]) -> None:
    text = "".join(json.dumps(line) + "\n" for line in lines)
    (out / "chaos-ledger.jsonl").write_bytes(text.encode())


def _change_in_data(out: Path, table: str, old: bytes, new: bytes) -> Path:
    for path in sorted((out / "data" / table).glob("*.csv")):
        data = path.read_bytes()
        if old in data:
            path.write_bytes(data.replace(old, new, 1))
            return path
    raise AssertionError(f"{old!r} not found")


def _checks(result) -> set[str]:  # type: ignore[no-untyped-def]
    return {problem.check for problem in result.problems}


def _messages(result) -> str:  # type: ignore[no-untyped-def]
    return "\n".join(problem.message for problem in result.problems)


# A clean run.


@pytest.mark.parametrize("formats", [("csv",), ("jsonl",), ("csv", "jsonl")])
def test_a_clean_run_is_ok(tmp_path: Path, formats: tuple[str, ...]) -> None:
    out = _run(tmp_path, _null(column="amount", count=3), formats=formats)
    result = validate_dir(out)
    assert result.ok
    assert result.ledger.total == 3
    assert result.ledger.verified == 3
    assert result.ledger.unverifiable == 0
    assert result.warnings == ()


def test_a_baseline_has_an_empty_ledger(tmp_path: Path) -> None:
    result = validate_dir(_run(tmp_path))
    assert result.ok
    assert result.ledger.total == 0


# Hashes and files.


def test_a_changed_byte_is_reported_on_the_file(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "data" / "orders" / "batch_0000.csv"
    data = bytearray(path.read_bytes())
    data[-5] = ord("0") if data[-5] != ord("0") else ord("1")
    path.write_bytes(bytes(data))
    result = validate_dir(out)
    assert not result.ok
    assert ("hash", "data/orders/batch_0000.csv") in {
        (problem.check, problem.path) for problem in result.problems
    }


def test_invalid_utf8_is_a_problem_and_not_a_crash(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(path.read_bytes() + b"\xff\xfe\n")
    result = validate_dir(out)
    assert {"hash", "columns"} <= _checks(result)


def test_a_missing_file_and_an_extra_file_are_problems(tmp_path: Path) -> None:
    out = _run(tmp_path)
    (out / "data" / "orders" / "batch_0001.csv").unlink()
    (out / "data" / "notes.txt").write_bytes(b"mine")
    result = validate_dir(out)
    messages = _messages(result)
    assert "the file is missing" in messages
    assert "is not listed in the manifest" in messages


def test_a_changed_ledger_is_a_hash_problem(tmp_path: Path) -> None:
    out = _run(tmp_path, _null(column="amount", count=1))
    path = out / "chaos-ledger.jsonl"
    path.write_bytes(path.read_bytes() + b"\n")
    assert ("hash", "chaos-ledger.jsonl") in {
        (problem.check, problem.path) for problem in validate_dir(out).problems
    }


def test_a_changed_manifest_hash_is_caught_by_content_sha256(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["content_sha256"] = "0" * 64
    path.write_bytes(json.dumps(manifest).encode())
    assert "content_sha256 does not match" in _messages(validate_dir(out))


@pytest.mark.parametrize(
    ("path", "safe"),
    [
        ("data/a.csv", True),
        ("../a.csv", False),
        ("data/../../a.csv", False),
        ("/etc/passwd", False),
        ("C:/a.csv", False),
        ("data\\a.csv", False),
        ("", False),
    ],
)
def test_is_safe_path(path: str, safe: bool) -> None:
    assert is_safe_path(path) is safe


def test_a_manifest_that_points_outside_is_reported_without_reading_it(tmp_path: Path) -> None:
    out = _run(tmp_path)
    (tmp_path / "secret.txt").write_bytes(b"secret")
    path = out / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["files"].append({"path": "../secret.txt", "sha256": "0" * 64})
    path.write_bytes(json.dumps(manifest).encode())
    assert "points outside the directory" in _messages(validate_dir(out))


# Not DataChaos output.


def test_a_directory_without_a_manifest_is_a_usage_error(tmp_path: Path) -> None:
    with pytest.raises(DataChaosError, match="has no chaos-manifest.json"):
        validate_dir(tmp_path)


@pytest.mark.parametrize(
    "content",
    [
        b"{nope",
        b"[]",
        b"{}",
        b'{"schema_version": "2.0"}',
        b'{"schema_version": "1.0", "files": 3}',
    ],
)
def test_an_invalid_manifest_is_a_usage_error(tmp_path: Path, content: bytes) -> None:
    (tmp_path / "chaos-manifest.json").write_bytes(content)
    with pytest.raises(DataChaosError):
        validate_dir(tmp_path)


# Columns and rows.


def test_a_wrong_header_is_reported(tmp_path: Path) -> None:
    out = _run(tmp_path)
    _change_in_data(out, "orders", b"status", b"state")
    _resign(out)
    result = validate_dir(out)
    assert "columns" in _checks(result)
    assert "does not match schema" in _messages(result)


def test_a_row_in_the_wrong_batch_is_reported(tmp_path: Path) -> None:
    out = _run(tmp_path)
    source = out / "data" / "orders" / "batch_0001.csv"
    target = out / "data" / "orders" / "batch_0002.csv"
    header, first, *rest = source.read_bytes().splitlines(keepends=True)
    source.write_bytes(b"".join([header, *rest]))
    target.write_bytes(target.read_bytes() + first)
    _resign(out)
    result = validate_dir(out)
    assert "belongs to batch 1" in _messages(result)
    assert "rows" in _checks(result)


def test_a_wrong_row_count_in_the_manifest_is_reported(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    entry = next(e for e in manifest["files"] if e["path"] == "data/orders/batch_0000.csv")
    entry["rows"] += 1
    path.write_bytes(json.dumps(manifest).encode())
    assert "the manifest says" in _messages(validate_dir(out))


# Schema changes.

RENAME = ScenarioConfig(
    "schema-drift",
    "orders",
    {
        "operation": "rename_column",
        "column": "source_system",
        "new_name": "channel",
        "from_batch": 1,
    },
)


@pytest.mark.parametrize(
    "params",
    [
        {"operation": "rename_column", "column": "source_system", "new_name": "channel"},
        {"operation": "drop_column", "column": "source_system"},
        {"operation": "add_column", "column": "region"},
        {"operation": "change_type", "column": "amount", "to": "string_comma_decimal"},
        {"operation": "change_type", "column": "amount", "to": "string"},
    ],
)
def test_every_schema_change_verifies_against_the_schema_files(
    tmp_path: Path, params: dict[str, object]
) -> None:
    out = _run(tmp_path, ScenarioConfig("schema-drift", "orders", {**params, "from_batch": 1}))
    result = validate_dir(out)
    assert result.ok, result.problems
    assert result.ledger.verified == 1


def test_a_schema_file_without_the_change_is_reported(tmp_path: Path) -> None:
    out = _run(tmp_path, RENAME)
    _write_ledger(out, [{**_ledger(out)[0], "batch": 2}])
    _resign(out)
    result = validate_dir(out)
    assert "schema_change" in _checks(result)
    assert "does not reflect the change of source_system" in _messages(result)


def test_a_change_type_line_with_another_type_is_reported(tmp_path: Path) -> None:
    drift = ScenarioConfig(
        "schema-drift",
        "orders",
        {"operation": "change_type", "column": "amount", "to": "string", "from_batch": 1},
    )
    out = _run(tmp_path, drift)
    _write_ledger(out, [{**_ledger(out)[0], "before": "integer"}])
    _resign(out)
    assert "schema_change" in _checks(validate_dir(out))


def test_schema_change_lines_are_not_treated_as_rows(tmp_path: Path) -> None:
    out = _run(tmp_path, RENAME, _null(column="amount", count=2))
    result = validate_dir(out)
    assert result.ok, result.problems
    assert result.ledger.verified == 3


# Unambiguous failures without row IDs.


def test_a_wrong_value_on_a_unique_key_is_exit_1(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    _change_in_data(out, "orders", b"UNKNOWN", b"created")
    _resign(out)
    result = validate_dir(out)
    assert not result.ok
    assert _checks(result) == {"ledger"}
    assert "status is 'created', not 'UNKNOWN'" in _messages(result)


def test_a_missing_row_with_a_unique_key_is_exit_1(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    entry = _ledger(out)[0]
    key = entry["key"]["order_id"].encode()  # type: ignore[index]
    for path in (out / "data" / "orders").glob("*.csv"):
        lines = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(line for line in lines if key not in line))
    _resign(out)
    assert "no row with key" in _messages(validate_dir(out))


def test_a_row_in_another_batch_than_the_line_says_is_exit_1(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    lines = _ledger(out)
    _write_ledger(out, [{**lines[0], "batch": (int(lines[0]["batch"]) + 1) % 3}])  # type: ignore[call-overload]
    _resign(out)
    assert "is in batch" in _messages(validate_dir(out))


def test_an_orphan_whose_value_has_a_parent_is_exit_1(tmp_path: Path) -> None:
    orphan = ScenarioConfig(
        "orphaned-foreign-key", "orders", {"mode": "deleted_parent", "count": 1}
    )
    out = _run(tmp_path, orphan)
    lines = _ledger(out)
    line = next(line for line in lines if line["action"] == "orphan_row")
    customers = {
        row.split(b",")[0].decode()
        for path in (out / "data" / "customers").glob("*.csv")
        for row in path.read_bytes().splitlines()[1:]
    }
    other = sorted(customers)[0]
    _change_in_data(out, "orders", str(line["before"]).encode(), other.encode())
    _write_ledger(out, [{**line, "before": other, "after": other}])
    _resign(out)
    assert "has a parent" in _messages(validate_dir(out))


def test_a_line_on_an_unknown_table_is_exit_1(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    _write_ledger(out, [{**_ledger(out)[0], "table": "nope"}])
    _resign(out)
    assert "unknown table 'nope'" in _messages(validate_dir(out))


@pytest.mark.parametrize("line", [b"[]\n", b"{}\n", b'{"table": "orders"}\n', b"3\n"])
def test_a_line_that_is_not_a_ledger_line_is_a_problem_and_not_a_crash(
    tmp_path: Path, line: bytes
) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    (out / "chaos-ledger.jsonl").write_bytes(line)
    _resign(out)
    assert "is not a ledger line" in _messages(validate_dir(out))


def test_a_schema_file_without_ingestion_time_is_a_problem_and_not_a_crash(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "schemas" / "orders" / "batch_0000.json"
    document = json.loads(path.read_bytes())
    for column in document["columns"]:
        if column["name"] == "ingested_at":
            column["role"] = "attribute"
    path.write_bytes(json.dumps(document).encode())
    _resign(out)
    assert "has no column with role ingestion_time" in _messages(validate_dir(out))


def test_a_broken_ledger_line_is_a_problem(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    (out / "chaos-ledger.jsonl").write_bytes(b"{nope\n")
    _resign(out)
    assert "cannot read the ledger" in _messages(validate_dir(out))


# Ambiguous without row IDs: exit 0 with a warning.


def test_duplicated_keys_are_unverifiable_with_a_warning(tmp_path: Path) -> None:
    duplicate = ScenarioConfig("duplicate-business-key", "orders", {"count": 3})
    result = validate_dir(_run(tmp_path, duplicate))
    assert result.ok
    assert result.ledger.unverifiable == 3
    assert result.ledger.verified == 0
    assert len(result.warnings) == 1
    assert "3 ledger lines cannot be verified" in result.warnings[0]
    assert "--emit-row-ids" in result.warnings[0]


def test_a_wrong_value_on_a_shared_key_is_not_reported_without_row_ids(tmp_path: Path) -> None:
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"count": 1, "mutation_mode": "mutate_non_key"}
    )
    out = _run(tmp_path, duplicate)
    line = _ledger(out)[0]
    _write_ledger(out, [{**line, "after": "not there"}])
    _resign(out)
    result = validate_dir(out)
    assert result.ok
    assert result.ledger.unverifiable == 1


def test_a_nulled_key_is_unverifiable(tmp_path: Path) -> None:
    out = _run(tmp_path, _null(column="order_id", count=2, allow_key=True))
    result = validate_dir(out)
    assert result.ok
    assert result.ledger.unverifiable == 2


def test_a_nulled_key_with_no_null_row_at_all_is_exit_1(tmp_path: Path) -> None:
    out = _run(tmp_path, _null(column="order_id", count=1, allow_key=True))
    line = _ledger(out)[0]
    order = str(line["before"]).encode()
    for path in (out / "data" / "orders").glob("*.csv"):
        data = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(
            b"".join(
                row.replace(b",", order + b",", 1) if row.startswith(b",") else row for row in data
            )
        )
    _resign(out)
    assert "no row with key" in _messages(validate_dir(out))


def test_a_delete_with_a_later_insert_of_the_same_key_is_unverifiable(tmp_path: Path) -> None:
    orphan = ScenarioConfig(
        "orphaned-foreign-key", "orders", {"mode": "deleted_parent", "count": 1}
    )
    out = _run(tmp_path, orphan)
    deleted = next(line for line in _ledger(out) if line["action"] == "delete_row")
    # The key is back on disk, and a later line says so: nothing here can be proved wrong.
    key = str(deleted["key"]["customer_id"])  # type: ignore[index]
    source = out / "data" / "customers" / "batch_0000.csv"
    first = source.read_bytes().splitlines(keepends=True)[1]
    source.write_bytes(source.read_bytes() + first.replace(first.split(b",")[0], key.encode(), 1))
    later = {
        **deleted,
        "action": "insert_row",
        "rid": "s0001-0000001",
        "source_rid": "x",
        "batch": 0,
    }
    _write_ledger(out, [deleted, later])
    _resign(out)
    result = validate_dir(out)
    assert not {"ledger"} & _checks(result)
    assert result.ledger.unverifiable >= 1
    assert any("a later line inserted the same key" in warning for warning in result.warnings)


def test_a_delete_with_the_key_on_disk_and_no_later_insert_is_exit_1(tmp_path: Path) -> None:
    orphan = ScenarioConfig(
        "orphaned-foreign-key", "orders", {"mode": "deleted_parent", "count": 1}
    )
    out = _run(tmp_path, orphan)
    deleted = next(line for line in _ledger(out) if line["action"] == "delete_row")
    key = str(deleted["key"]["customer_id"])  # type: ignore[index]
    source = out / "data" / "customers" / "batch_0000.csv"
    first = source.read_bytes().splitlines(keepends=True)[1]
    source.write_bytes(source.read_bytes() + first.replace(first.split(b",")[0], key.encode(), 1))
    _write_ledger(out, [deleted])
    _resign(out)
    assert "is still there" in _messages(validate_dir(out))


def test_an_unreadable_data_file_makes_lines_unverifiable_and_not_failed(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1))
    path = out / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(b"\xff\xfe")
    result = validate_dir(out)
    assert "columns" in _checks(result)
    assert "ledger" not in _checks(result)
    assert result.ledger.unverifiable == 1
    assert any("could not be read" in warning for warning in result.warnings)


# With row IDs everything is exact.


def test_duplicates_are_verified_exactly_with_row_ids(tmp_path: Path) -> None:
    duplicate = ScenarioConfig("duplicate-business-key", "orders", {"count": 3})
    result = validate_dir(_run(tmp_path, duplicate, row_ids=True))
    assert result.ok
    assert result.ledger.verified == 3
    assert result.ledger.unverifiable == 0
    assert result.warnings == ()


def test_a_nulled_key_is_verified_exactly_with_row_ids(tmp_path: Path) -> None:
    out = _run(tmp_path, _null(column="order_id", count=2, allow_key=True), row_ids=True)
    result = validate_dir(out)
    assert result.ok
    assert result.ledger.verified == 2


def test_a_wrong_value_on_a_shared_key_is_exit_1_with_row_ids(tmp_path: Path) -> None:
    duplicate = ScenarioConfig(
        "duplicate-business-key", "orders", {"count": 1, "mutation_mode": "mutate_non_key"}
    )
    out = _run(tmp_path, duplicate, row_ids=True)
    _write_ledger(out, [{**_ledger(out)[0], "after": "not there"}])
    _resign(out)
    result = validate_dir(out)
    assert not result.ok
    assert "not 'not there'" in _messages(result)


def test_a_missing_row_id_is_exit_1(tmp_path: Path) -> None:
    out = _run(tmp_path, _enum(column="status", count=1), row_ids=True)
    _write_ledger(out, [{**_ledger(out)[0], "rid": "ghost"}])
    _resign(out)
    assert "no row with key" in _messages(validate_dir(out))


def test_a_deleted_row_that_is_still_there_is_exit_1_with_row_ids(tmp_path: Path) -> None:
    orphan = ScenarioConfig(
        "orphaned-foreign-key", "orders", {"mode": "deleted_parent", "count": 1}
    )
    out = _run(tmp_path, orphan, row_ids=True)
    deleted = next(line for line in _ledger(out) if line["action"] == "delete_row")
    customers = out / "data" / "customers" / "batch_0000.csv"
    rid = customers.read_bytes().splitlines()[1].split(b",")[-1].decode()
    _write_ledger(out, [{**deleted, "rid": rid}])
    _resign(out)
    assert "is still there" in _messages(validate_dir(out))


# allow_overlap: only the last line per row and column counts.


def test_earlier_lines_on_the_same_row_and_column_are_superseded(tmp_path: Path) -> None:
    first = _enum(column="source_system", rate=1.0, variant="whitespace")
    second = _null(column="source_system", count=5, allow_overlap=True)
    result = validate_dir(_run(tmp_path, first, second, row_ids=True))
    assert result.ok, result.problems
    assert result.ledger.superseded == 5
    assert result.ledger.verified == 100


def test_a_row_moved_by_a_later_scenario_is_checked_in_its_final_batch(tmp_path: Path) -> None:
    first = _enum(column="status", rate=1.0)
    late = ScenarioConfig(
        "late-arriving-data",
        "orders",
        {"rate": 1.0, "delay_min": "1d", "delay_max": "1d", "allow_overlap": True},
    )
    result = validate_dir(_run(tmp_path, first, late))
    assert result.ok, result.problems
    assert result.ledger.unverifiable == 0


def test_lines_on_a_row_deleted_later_are_superseded(tmp_path: Path) -> None:
    earlier = ScenarioConfig("invalid-enum", "customers", {"column": "country_code", "rate": 1.0})
    orphan = ScenarioConfig(
        "orphaned-foreign-key",
        "orders",
        {"mode": "deleted_parent", "count": 2, "allow_overlap": True},
    )
    result = validate_dir(_run(tmp_path, earlier, orphan))
    assert result.ok, result.problems
    assert result.ledger.superseded >= 2


# All kinds of line.


def test_lines_of_every_scenario_verify(tmp_path: Path) -> None:
    out = _run(
        tmp_path,
        RENAME,
        _null(column="amount", count=3),
        ScenarioConfig("orphaned-foreign-key", "orders", {"mode": "deleted_parent", "count": 2}),
        ScenarioConfig("orphaned-foreign-key", "order_events", {"count": 3}),
        ScenarioConfig(
            "late-arriving-data", "order_events", {"count": 5, "delay_min": "1d", "delay_max": "1d"}
        ),
        ScenarioConfig("out-of-order-events", "order_events", {"count": 3}),
    )
    result = validate_dir(out)
    assert result.ok, result.problems
    assert result.ledger.unverifiable == 0
    assert result.ledger.verified == result.ledger.total


# --reproduce.


def test_reproduce_is_ok_on_a_clean_run(tmp_path: Path) -> None:
    out = _run(tmp_path, _null(column="amount", count=3))
    result = validate_dir(out, reproduce=True)
    assert result.ok
    assert result.reproduce


def test_reproduce_finds_data_changed_with_consistent_hashes(tmp_path: Path) -> None:
    out = _run(tmp_path, _null(column="amount", count=3))
    _change_in_data(out, "customers", b"true", b"false")
    _resign(out)
    assert validate_dir(out).ok
    result = validate_dir(out, reproduce=True)
    assert _checks(result) == {"reproduce"}
    assert "content_sha256 differs" in _messages(result)
    assert any(problem.path.startswith("data/customers/") for problem in result.problems)


def test_reproduce_also_covers_schema_files(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "schemas" / "orders" / "batch_0000.json"
    document = json.loads(path.read_bytes())
    document["tool_version"] = "0"
    path.write_bytes(json.dumps(document).encode())
    _resign(out)
    result = validate_dir(out, reproduce=True)
    assert ("reproduce", "schemas/orders/batch_0000.json") in {
        (problem.check, problem.path) for problem in result.problems
    }


def test_reproduce_mentions_another_version(tmp_path: Path) -> None:
    out = _run(tmp_path)
    _change_in_data(out, "customers", b"true", b"false")
    path = out / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["tool_version"] = "0.0.1"
    path.write_bytes(json.dumps(manifest).encode())
    _resign(out)
    assert "written by 0.0.1" in _messages(validate_dir(out, reproduce=True))


def test_reproduce_reports_a_broken_config(tmp_path: Path) -> None:
    out = _run(tmp_path)
    path = out / "chaos-manifest.json"
    manifest = json.loads(path.read_bytes())
    manifest["run"]["config"]["seed"] = "many"
    path.write_bytes(json.dumps(manifest).encode())
    result = validate_dir(out, reproduce=True)
    assert "reproduce" in _checks(result)


def test_reproduce_reports_a_broken_invariant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = _run(tmp_path, _null(column="amount", count=3))
    scenario = engine.SCENARIOS["null-critical-column"]
    monkeypatch.setattr(type(scenario), "check", lambda self, ctx, outcome: ["it does not hold"])
    result = validate_dir(out, reproduce=True)
    assert "broke its invariant: it does not hold" in _messages(result)
    assert validate_dir(out).ok


def test_reproduce_leaves_nothing_next_to_the_input(tmp_path: Path) -> None:
    out = _run(tmp_path)
    before = sorted(path.name for path in tmp_path.iterdir())
    validate_dir(out, reproduce=True)
    assert sorted(path.name for path in tmp_path.iterdir()) == before


# The command.


def test_cli_ok_with_a_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    out = _run(tmp_path, _null(column="amount", count=2))
    assert cli.main(["validate", str(out)]) == 0
    captured = capsys.readouterr()
    assert f"ok: {out} (files and ledger)" in captured.out
    assert "  ledger: 2 lines, 2 verified, 0 superseded, 0 unverifiable\n" in captured.out
    assert captured.err == ""


def test_cli_reproduce_says_so(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["validate", str(_run(tmp_path)), "--reproduce"]) == 0
    assert "(files and ledger, and reproduced)" in capsys.readouterr().out


def test_cli_failure_is_exit_1_and_names_the_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(tmp_path)
    path = out / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(path.read_bytes() + b"x")
    assert cli.main(["validate", str(out)]) == 1
    captured = capsys.readouterr()
    assert "hash: data/orders/batch_0000.csv: the file does not match its hash" in captured.out
    assert f"failed: {out}" in captured.out


def test_cli_unverifiable_is_exit_0_with_a_warning_and_a_count(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(tmp_path, ScenarioConfig("duplicate-business-key", "orders", {"count": 2}))
    assert cli.main(["validate", str(out)]) == 0
    captured = capsys.readouterr()
    assert "2 unverifiable" in captured.out
    assert captured.err.startswith("datachaos: warning: 2 ledger lines cannot be verified")


def test_cli_json_has_versions_and_the_count(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(tmp_path, ScenarioConfig("duplicate-business-key", "orders", {"count": 2}))
    assert cli.main(["validate", str(out), "--output", "json"]) == 0
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert list(document) == [
        "schema_version",
        "tool_version",
        "status",
        "directory",
        "reproduce",
        "ledger",
        "unverifiable",
        "problems",
        "warnings",
    ]
    assert document["schema_version"] == "1.0"
    assert document["tool_version"] == __version__
    assert document["status"] == "ok"
    assert document["unverifiable"] == 2
    assert document["ledger"] == {"total": 2, "superseded": 0, "verified": 0, "unverifiable": 2}
    assert len(document["warnings"]) == 1
    assert captured.err.startswith("datachaos: warning:")


def test_cli_json_failure_is_valid_json_and_exit_1(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = _run(tmp_path)
    path = out / "data" / "orders" / "batch_0000.csv"
    path.write_bytes(path.read_bytes() + b"x")
    assert cli.main(["validate", str(out), "--output", "json"]) == 1
    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "failed"
    assert {"check": "hash", "path": "data/orders/batch_0000.csv"}.items() <= document["problems"][
        0
    ].items()


def test_cli_not_a_datachaos_directory_is_exit_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(["validate", str(tmp_path), "--output", "json"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("datachaos: error:")


def test_help_explains_the_difference(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        cli.main(["validate", "--help"])
    text = " ".join(capsys.readouterr().out.split())
    assert "only what can be decided from the files on disk" in text
    assert "unverifiable" in text
    assert "invariant of every scenario" in text


def test_helper_config_is_valid(tmp_path: Path) -> None:
    engine.run(config(), tmp_path / "out")
    assert validate_dir(tmp_path / "out").ok
