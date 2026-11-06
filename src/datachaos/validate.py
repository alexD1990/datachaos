"""Checks DataChaos output.

Without `reproduce`, only what can be decided from the files on disk: a line that cannot be proved
wrong is never a problem, only unverifiable. With `reproduce`, the config in the manifest runs
again, and the invariant of every scenario is checked in that run.
"""

from __future__ import annotations

import json
import tempfile
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from datachaos import engine
from datachaos._version import __version__
from datachaos.config import parse_config
from datachaos.errors import DataChaosError
from datachaos.io.output_dir import sha256_file
from datachaos.io.reader import read_csv, read_json, read_jsonl, read_schema
from datachaos.model.manifest import (
    LEDGER_FILE,
    MANIFEST_FILE,
    FileEntry,
    compute_content_sha256,
)
from datachaos.model.schema import Column, Role, TableSchema
from datachaos.model.table import Value
from datachaos.timeutil import batch_index, parse_duration, parse_timestamp, window_start

ROW_ID = "_chaos_rid"
_READERS = {"csv": read_csv, "jsonl": read_jsonl}
_SHARED_KEY = "several rows share the key, or the key is NULL"
_REINSERTED = "a later line inserted the same key"
_UNREADABLE = "a file it depends on could not be read"
_UNKNOWN = "the action is not known to this version"


@dataclass(frozen=True)
class Problem:
    check: str
    path: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"check": self.check, "path": self.path, "message": self.message}


@dataclass(frozen=True)
class LedgerCounts:
    total: int = 0
    superseded: int = 0
    verified: int = 0
    unverifiable: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "total": self.total,
            "superseded": self.superseded,
            "verified": self.verified,
            "unverifiable": self.unverifiable,
        }


@dataclass(frozen=True)
class ValidationResult:
    directory: str
    reproduce: bool
    problems: tuple[Problem, ...]
    ledger: LedgerCounts
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.problems

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "ok" if self.ok else "failed",
            "directory": self.directory,
            "reproduce": self.reproduce,
            "ledger": self.ledger.to_dict(),
            "unverifiable": self.ledger.unverifiable,
            "problems": [problem.to_dict() for problem in self.problems],
            "warnings": list(self.warnings),
        }


def load_manifest(directory: Path) -> dict[str, Any]:
    """The manifest of a DataChaos directory. Raises DataChaosError if it is not one."""
    path = directory / MANIFEST_FILE
    if not path.is_file():
        raise DataChaosError(f"{directory} has no {MANIFEST_FILE}, so it is not DataChaos output")
    manifest = read_json(path)
    try:
        version = manifest["schema_version"]
        if not isinstance(version, str) or not version.startswith("1."):
            raise DataChaosError(f"{path}: schema_version {version!r} is not supported")
        for entry in manifest["files"]:
            if not isinstance(entry["path"], str) or not isinstance(entry["sha256"], str):
                raise TypeError(entry)
        tables = manifest["dataset"]["tables"]
        if not isinstance(tables, dict) or not isinstance(manifest["dataset"]["batches"], int):
            raise TypeError(tables)
        for name in ("as_of", "batch_interval"):
            if not isinstance(manifest["dataset"][name], str):
                raise TypeError(name)
        if not isinstance(manifest["run"]["config"], dict):
            raise TypeError("config")
        manifest["content_sha256"]
    except (KeyError, TypeError) as exc:
        raise DataChaosError(f"{path} is not a valid manifest: {exc!r}") from exc
    return manifest


def is_safe_path(path: str) -> bool:
    """A manifest must not point outside its directory."""
    pure = PurePosixPath(path)
    return (
        path != ""
        and "\\" not in path
        and not pure.is_absolute()
        and ".." not in pure.parts
        and ":" not in path
    )


def check_hashes(directory: Path, manifest: Mapping[str, Any]) -> list[Problem]:
    problems = []
    listed = {entry["path"]: entry for entry in manifest["files"]}
    for path in sorted(listed):
        if not is_safe_path(path):
            problems.append(Problem("hash", path, "the manifest points outside the directory"))
        elif not (directory / path).is_file():
            problems.append(Problem("hash", path, "the file is missing"))
        elif sha256_file(directory / path) != listed[path]["sha256"]:
            problems.append(Problem("hash", path, "the file does not match its hash"))
    for found in sorted(directory.rglob("*")):
        relative = found.relative_to(directory).as_posix()
        if not found.is_dir() and relative != MANIFEST_FILE and relative not in listed:
            problems.append(Problem("hash", relative, "the file is not listed in the manifest"))
    entries = [FileEntry(entry["path"], entry["sha256"]) for entry in manifest["files"]]
    if compute_content_sha256(entries) != manifest["content_sha256"]:
        problems.append(
            Problem("hash", MANIFEST_FILE, "content_sha256 does not match the listed files")
        )
    return problems


@dataclass
class _TableOnDisk:
    """What could be read of one table."""

    schemas: dict[int, TableSchema]
    rows: list[tuple[int, dict[str, Value]]]
    complete: bool = True

    @property
    def has_row_ids(self) -> bool:
        return bool(self.schemas) and all(
            any(column.name == ROW_ID for column in schema.columns)
            for schema in self.schemas.values()
        )

    @property
    def key_columns(self) -> list[str]:
        for schema in self.schemas.values():
            return [column.name for column in schema.columns if column.role is Role.KEY]
        return []


def _read_tables(
    directory: Path, manifest: Mapping[str, Any], problems: list[Problem]
) -> dict[str, _TableOnDisk]:
    dataset = manifest["dataset"]
    batches = dataset["batches"]
    interval = parse_duration(dataset["batch_interval"])
    start = window_start(parse_timestamp(dataset["as_of"]), batches, interval)
    formats = manifest["run"]["config"].get("output", {}).get("formats", ["csv"])
    listed = {entry["path"]: entry for entry in manifest["files"]}
    tables = {}
    for name, counts in dataset["tables"].items():
        table = _TableOnDisk({}, [])
        tables[name] = table
        for batch in range(batches):
            stem = f"batch_{batch:04d}"
            schema_path = f"schemas/{name}/{stem}.json"
            try:
                schema = read_schema(directory / schema_path)
            except DataChaosError as exc:
                problems.append(Problem("columns", schema_path, str(exc)))
                table.complete = False
                continue
            table.schemas[batch] = schema
            for position, file_format in enumerate(formats):
                path = f"data/{name}/{stem}.{file_format}"
                try:
                    rows = _READERS[file_format](directory / path, schema)
                except (DataChaosError, KeyError) as exc:
                    problems.append(Problem("columns", path, str(exc)))
                    if position == 0:
                        table.complete = False
                    continue
                expected = listed.get(path, {}).get("rows")
                if expected is not None and expected != len(rows):
                    problems.append(
                        Problem("rows", path, f"the manifest says {expected} rows, not {len(rows)}")
                    )
                _check_batch_of_rows(schema, rows, batch, start, interval, path, problems)
                if position == 0:
                    table.rows.extend((batch, row) for row in rows)
        final = counts.get("rows_final")
        if table.complete and final is not None and final != len(table.rows):
            problems.append(
                Problem(
                    "rows", f"data/{name}", f"the manifest says {final} rows, not {len(table.rows)}"
                )
            )
    return tables


def _check_batch_of_rows(
    schema: TableSchema,
    rows: list[dict[str, Value]],
    batch: int,
    start: int,
    interval: int,
    path: str,
    problems: list[Problem],
) -> None:
    try:
        column = schema.column_with_role(Role.INGESTION_TIME).name
    except ValueError as exc:
        problems.append(Problem("columns", path, str(exc)))
        return
    for number, row in enumerate(rows, 1):
        try:
            actual = batch_index(parse_timestamp(str(row[column])), start, interval)
        except ValueError as exc:
            problems.append(Problem("rows", path, f"row {number}: {exc}"))
            continue
        if actual != batch:
            problems.append(
                Problem("rows", path, f"row {number}: {column} belongs to batch {actual}")
            )


def _read_ledger(directory: Path, problems: list[Problem]) -> list[dict[str, Any]] | None:
    try:
        with open(directory / LEDGER_FILE, encoding="utf-8", newline="") as file:
            lines = [json.loads(line) for line in file]
    except (OSError, ValueError) as exc:
        problems.append(Problem("ledger", LEDGER_FILE, f"cannot read the ledger: {exc}"))
        return None
    for number, line in enumerate(lines, 1):
        if not _is_ledger_line(line):
            problems.append(
                Problem("ledger", f"{LEDGER_FILE}:{number}", "the line is not a ledger line")
            )
            return None
    return lines


def _is_ledger_line(line: object) -> bool:
    return (
        isinstance(line, dict)
        and isinstance(line.get("table"), str)
        and isinstance(line.get("action"), str)
        and isinstance(line.get("rid"), str)
        and isinstance(line.get("key"), dict)
        and isinstance(line.get("batch"), int)
        and {"column", "before", "after"} <= set(line)
    )


def _check_schema_change(
    entry: Mapping[str, Any], table: _TableOnDisk, problems: list[Problem]
) -> str:
    """Returns the outcome of the line: verified, or the reason it cannot be verified."""
    if not table.schemas or not table.complete:
        return _UNREADABLE
    wrong = 0
    for batch, schema in table.schemas.items():
        after_change = batch >= entry["batch"]
        columns = {column.name: column for column in schema.columns}
        if not _schema_matches(entry, columns, after_change):
            wrong += 1
            problems.append(
                Problem(
                    "schema_change",
                    f"schemas/{entry['table']}/batch_{batch:04d}.json",
                    f"does not reflect the change of {entry['column']} from batch {entry['batch']}",
                )
            )
    return "failed" if wrong else "verified"


def _schema_matches(
    entry: Mapping[str, Any], columns: Mapping[str, Column], after_change: bool
) -> bool:
    name, before, after = entry["column"], entry["before"], entry["after"]
    if before is None:
        return (after in columns) == after_change
    if after is None:
        return (before in columns) != after_change
    if before == name:
        return (after in columns) == after_change and (before in columns) != after_change
    if name not in columns:
        return False
    return columns[name].type.value == ("string" if after_change else before)


def _check_ledger(
    lines: list[dict[str, Any]], tables: Mapping[str, _TableOnDisk], problems: list[Problem]
) -> tuple[LedgerCounts, Counter[str]]:
    last_per_column: dict[tuple[str, str, str | None], int] = {}
    last_per_row: dict[tuple[str, str], int] = {}
    for index, entry in enumerate(lines):
        if entry.get("action") != "schema_change":
            last_per_column[(entry["table"], entry["rid"], entry.get("column"))] = index
            last_per_row[(entry["table"], entry["rid"])] = index

    reasons: Counter[str] = Counter()
    verified = superseded = 0
    for index, entry in enumerate(lines):
        table = tables.get(entry.get("table", ""))
        path = f"{LEDGER_FILE}:{index + 1}"
        if table is None:
            problems.append(Problem("ledger", path, f"unknown table {entry.get('table')!r}"))
            continue
        action = entry.get("action")
        if action == "schema_change":
            outcome = _check_schema_change(entry, table, problems)
        else:
            row_key = (entry["table"], entry["rid"])
            is_deleted = lines[last_per_row[row_key]]["action"] == "delete_row"
            if (is_deleted and action != "delete_row") or (
                last_per_column[(entry["table"], entry["rid"], entry.get("column"))] != index
            ):
                superseded += 1
                continue
            final_batch = lines[last_per_row[row_key]]["batch"]
            outcome = _check_line(entry, index, final_batch, table, tables, lines, path, problems)
        if outcome == "verified":
            verified += 1
        elif outcome != "failed":
            reasons[outcome] += 1
    counts = LedgerCounts(len(lines), superseded, verified, sum(reasons.values()))
    return counts, reasons


def _check_line(
    entry: Mapping[str, Any],
    index: int,
    final_batch: int,
    table: _TableOnDisk,
    tables: Mapping[str, _TableOnDisk],
    lines: list[dict[str, Any]],
    path: str,
    problems: list[Problem],
) -> str:
    action = entry["action"]
    if action not in ("insert_row", "delete_row", "set_value", "move_batch", "orphan_row"):
        return _UNKNOWN
    if not table.complete:
        return _UNREADABLE
    if table.has_row_ids:
        found = [item for item in table.rows if item[1].get(ROW_ID) == entry["rid"]]
    else:
        key = _effective_key(entry, lines, table.key_columns)
        found = [item for item in table.rows if _key_of(item[1], table.key_columns) == key]
        if action == "delete_row":
            if found and any(
                line.get("action") == "insert_row" and line.get("key") == entry["key"]
                for line in lines[index + 1 :]
                if line.get("table") == entry["table"]
            ):
                return _REINSERTED
        elif found and (len(found) > 1 or any(value is None for value in key)):
            return _SHARED_KEY
    if action == "delete_row":
        if found:
            problems.append(Problem("ledger", path, f"deleted key {entry['key']} is still there"))
            return "failed"
        return "verified"
    if not found:
        problems.append(Problem("ledger", path, f"no row with key {entry['key']}"))
        return "failed"
    batch, row = found[0]
    if batch != final_batch:
        problems.append(
            Problem("ledger", path, f"key {entry['key']} is in batch {batch}, not {final_batch}")
        )
        return "failed"
    column = entry.get("column")
    if column is not None and action != "move_batch" and row.get(column) != entry["after"]:
        problems.append(
            Problem(
                "ledger",
                path,
                f"key {entry['key']}: {column} is {row.get(column)!r}, not {entry['after']!r}",
            )
        )
        return "failed"
    if action == "orphan_row":
        return _check_orphan(entry, row, batch, table, tables, path, problems)
    return "verified"


def _check_orphan(
    entry: Mapping[str, Any],
    row: Mapping[str, Value],
    batch: int,
    table: _TableOnDisk,
    tables: Mapping[str, _TableOnDisk],
    path: str,
    problems: list[Problem],
) -> str:
    column = next(item for item in table.schemas[batch].columns if item.name == entry["column"])
    parent_table, _, parent_column = (column.references or "").partition(".")
    parent = tables.get(parent_table)
    if parent is None or not parent.complete:
        return _UNREADABLE
    value = row.get(entry["column"])
    if any(item[1].get(parent_column) == value for item in parent.rows):
        problems.append(Problem("ledger", path, f"orphan value {value!r} has a parent"))
        return "failed"
    return "verified"


def _key_of(row: Mapping[str, Value], columns: list[str]) -> tuple[Value, ...]:
    return tuple(row.get(column) for column in columns)


def _effective_key(
    entry: Mapping[str, Any], lines: list[dict[str, Any]], columns: list[str]
) -> tuple[Value, ...]:
    """The key of the row now. A later line may have set a key column to NULL."""
    values = dict(entry["key"])
    for line in lines:
        if (
            line.get("action") == "set_value"
            and line.get("table") == entry["table"]
            and line.get("rid") == entry["rid"]
            and line.get("column") in columns
        ):
            values[line["column"]] = line["after"]
    return tuple(values.get(column) for column in columns)


def _reproduce(manifest: Mapping[str, Any], problems: list[Problem]) -> None:
    version_note = ""
    if manifest.get("tool_version") != __version__:
        version_note = (
            f" (this is DataChaos {__version__}, and it was written by "
            f"{manifest.get('tool_version')})"
        )
    with tempfile.TemporaryDirectory(prefix="datachaos-reproduce-") as temp:
        try:
            config = parse_config(manifest["run"]["config"])
            again = engine.run(config, Path(temp) / "out")
        except DataChaosError as exc:
            problems.append(Problem("reproduce", MANIFEST_FILE, f"{exc}{version_note}"))
            return
    if again.content_sha256 != manifest["content_sha256"]:
        problems.append(
            Problem("reproduce", MANIFEST_FILE, f"content_sha256 differs{version_note}")
        )
    listed = {entry["path"]: entry["sha256"] for entry in manifest["files"]}
    produced = {entry.path: entry.sha256 for entry in again.files}
    for path in sorted(set(listed) | set(produced)):
        if listed.get(path) != produced.get(path):
            problems.append(Problem("reproduce", path, f"the file differs{version_note}"))


def validate_dir(directory: Path, *, reproduce: bool = False) -> ValidationResult:
    """Raises DataChaosError if `directory` is not DataChaos output. Other findings are returned."""
    manifest = load_manifest(directory)
    problems = check_hashes(directory, manifest)
    try:
        tables = _read_tables(directory, manifest, problems)
    except (KeyError, TypeError, ValueError) as exc:
        raise DataChaosError(f"{directory / MANIFEST_FILE} is not a valid manifest: {exc}") from exc
    counts = LedgerCounts()
    reasons: Counter[str] = Counter()
    lines = _read_ledger(directory, problems)
    if lines is not None:
        counts, reasons = _check_ledger(lines, tables, problems)
    if reproduce:
        _reproduce(manifest, problems)
    warnings = tuple(
        f"{count} ledger lines cannot be verified: {reason}"
        + (" (--emit-row-ids makes them exact)" if reason == _SHARED_KEY else "")
        for reason, count in sorted(reasons.items())
    )
    return ValidationResult(str(directory), reproduce, tuple(problems), counts, warnings)
