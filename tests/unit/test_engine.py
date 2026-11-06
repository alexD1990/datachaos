from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest

from datachaos import engine, generate
from datachaos._version import __version__
from datachaos.config import DatasetConfig, OutputConfig, RunConfig
from datachaos.datasets.shop import build_shop
from datachaos.errors import DataChaosError
from datachaos.io.output_dir import sha256_file
from datachaos.io.reader import read_csv, read_json, read_jsonl, read_schema
from datachaos.model.manifest import FileEntry, compute_content_sha256
from datachaos.timeutil import batch_index, parse_duration, parse_timestamp, window_start

REPO = Path(__file__).resolve().parents[2]
SMALL = DatasetConfig(customers=20, orders=50, batches=3)


def _manifest(out: Path) -> dict[str, Any]:
    return read_json(out / "chaos-manifest.json")


def _validate(document: dict[str, Any]) -> None:
    schema = read_json(REPO / "schema" / "chaos-manifest-1.0.schema.json")
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema).validate(document)


def test_manifest_validates_against_the_schema(tmp_path: Path) -> None:
    engine.generate(RunConfig(dataset=SMALL), tmp_path / "out")
    document = _manifest(tmp_path / "out")
    _validate(document)
    assert document["schema_version"] == "1.0"
    assert document["tool_version"] == __version__
    assert document["run"]["seed"] == 42
    assert document["run"]["config"] == RunConfig(dataset=SMALL).to_dict()
    assert document["scenarios"] == []
    assert document["warnings"] == []


def test_schema_rejects_a_manifest_without_required_fields(tmp_path: Path) -> None:
    engine.generate(RunConfig(dataset=SMALL), tmp_path / "out")
    document = _manifest(tmp_path / "out")
    del document["content_sha256"]
    with pytest.raises(jsonschema.ValidationError):
        _validate(document)


def test_files_in_the_manifest_match_the_disk(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(RunConfig(dataset=SMALL, output=OutputConfig(formats=("csv", "jsonl"))), out)
    document = _manifest(out)
    listed = {entry["path"]: entry for entry in document["files"]}
    on_disk = {
        path.relative_to(out).as_posix()
        for path in out.rglob("*")
        if path.is_file() and path.name != "chaos-manifest.json"
    }
    assert set(listed) == on_disk
    assert [entry["path"] for entry in document["files"]] == sorted(listed)
    for path, entry in listed.items():
        assert "\\" not in path
        assert entry["sha256"] == sha256_file(out / path)
        if path.startswith("schemas/"):
            assert "rows" not in entry
    assert listed["chaos-ledger.jsonl"]["rows"] == 0
    assert (out / "chaos-ledger.jsonl").read_bytes() == b""
    assert document["content_sha256"] == compute_content_sha256(
        FileEntry(entry["path"], entry["sha256"]) for entry in document["files"]
    )


def test_row_counts_add_up_and_files_hold_the_right_batch(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(RunConfig(dataset=SMALL), out)
    document = _manifest(out)
    start = window_start(
        parse_timestamp(SMALL.as_of), SMALL.batches, parse_duration(SMALL.batch_interval)
    )
    listed = {entry["path"]: entry for entry in document["files"]}
    for table, counts in document["dataset"]["tables"].items():
        total = 0
        for k in range(SMALL.batches):
            schema = read_schema(out / "schemas" / table / f"batch_{k:04d}.json")
            rows = read_csv(out / "data" / table / f"batch_{k:04d}.csv", schema)
            assert listed[f"data/{table}/batch_{k:04d}.csv"]["rows"] == len(rows)
            total += len(rows)
            for row in rows:
                ingested_at = parse_timestamp(str(row["ingested_at"]))
                assert batch_index(ingested_at, start, 86400) == k
        assert counts == {"rows_baseline": total, "rows_final": total}


@pytest.mark.parametrize(("file_format", "reader"), [("csv", read_csv), ("jsonl", read_jsonl)])
def test_files_read_back_equal_to_the_generated_rows(tmp_path: Path, file_format, reader) -> None:  # type: ignore[no-untyped-def]
    out = tmp_path / "out"
    config = RunConfig(seed=9, dataset=SMALL, output=OutputConfig(formats=(file_format,)))
    engine.generate(config, out)
    dataset = build_shop(
        seed=9,
        as_of=SMALL.as_of,
        customers=SMALL.customers,
        orders=SMALL.orders,
        batches=SMALL.batches,
        batch_interval=SMALL.batch_interval,
    )
    for name, table in dataset.tables.items():
        batches = engine.partition(table, dataset)
        for batch in batches:
            path = out / "data" / name / f"batch_{batch.index:04d}.{file_format}"
            assert reader(path, batch.schema) == [dict(row.values) for row in batch.rows]


def test_every_batch_has_files_even_when_empty(tmp_path: Path) -> None:
    out = tmp_path / "out"
    settings = DatasetConfig(customers=1, orders=1, batches=10)
    engine.generate(RunConfig(dataset=settings), out)
    sizes = [
        len((out / "data" / "customers" / f"batch_{k:04d}.csv").read_bytes().splitlines())
        for k in range(10)
    ]
    assert sorted(sizes) == [1] * 9 + [2]
    for k in range(10):
        assert (out / "schemas" / "customers" / f"batch_{k:04d}.json").exists()


def test_schema_file_has_the_required_versions(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(RunConfig(dataset=SMALL), out)
    document = read_json(out / "schemas" / "orders" / "batch_0002.json")
    assert document["schema_version"] == "1.0"
    assert document["tool_version"] == __version__
    assert document["table"] == "orders"
    assert document["kind"] == "entity"
    assert document["batch"] == 2


def test_emit_row_ids_adds_the_column_to_data_and_schema(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(RunConfig(dataset=SMALL, output=OutputConfig(emit_row_ids=True)), out)
    schema = read_schema(out / "schemas" / "orders" / "batch_0000.json")
    assert schema.columns[-1].name == "_chaos_rid"
    rows = read_csv(out / "data" / "orders" / "batch_0000.csv", schema)
    assert rows
    assert all(str(row["_chaos_rid"]).startswith("o") for row in rows)
    assert all(row["_chaos_rid"] == f"o{str(row['order_id'])[1:]}" for row in rows)


def test_row_ids_are_off_by_default(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(RunConfig(dataset=SMALL), out)
    assert b"_chaos_rid" not in (out / "data" / "orders" / "batch_0000.csv").read_bytes()


def test_combined_holds_all_batches_in_order(tmp_path: Path) -> None:
    out = tmp_path / "out"
    config = RunConfig(dataset=SMALL, output=OutputConfig(formats=("csv", "jsonl"), combined=True))
    engine.generate(config, out)
    for table in ("customers", "orders", "order_events"):
        header, *combined = (out / "data" / f"{table}.csv").read_bytes().splitlines()
        parts = []
        for k in range(SMALL.batches):
            part = (out / "data" / table / f"batch_{k:04d}.csv").read_bytes().splitlines()
            assert part[0] == header
            parts.extend(part[1:])
        assert combined == parts
        assert (out / "data" / f"{table}.jsonl").exists()


def test_no_combined_files_by_default(tmp_path: Path) -> None:
    out = tmp_path / "out"
    engine.generate(RunConfig(dataset=SMALL), out)
    assert not (out / "data" / "orders.csv").exists()


def test_combined_refuses_a_schema_that_varies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = engine.partition

    def varying(table, dataset):  # type: ignore[no-untyped-def]
        batches = real(table, dataset)
        changed = engine._with_row_ids(batches[-1])
        return (*batches[:-1], changed)

    monkeypatch.setattr(engine, "partition", varying)
    config = RunConfig(dataset=SMALL, output=OutputConfig(combined=True))
    with pytest.raises(DataChaosError, match="schema differs between batches"):
        engine.generate(config, tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_invalid_config_writes_nothing(tmp_path: Path) -> None:
    out = tmp_path / "parent" / "out"
    with pytest.raises(DataChaosError, match="dataset.orders"):
        engine.generate(RunConfig(dataset=DatasetConfig(orders=0)), out)
    assert list(tmp_path.iterdir()) == []


def test_failure_while_writing_leaves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(path: Path, document: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(engine, "write_json", fail)
    with pytest.raises(RuntimeError):
        engine.generate(RunConfig(dataset=SMALL), tmp_path / "out")
    assert list(tmp_path.iterdir()) == []


def test_row_outside_the_window_is_an_internal_error() -> None:
    dataset = build_shop(
        seed=1,
        as_of="2026-01-01T00:00:00Z",
        customers=2,
        orders=2,
        batches=1,
        batch_interval="1d",
    )
    moved = type(dataset)(
        name=dataset.name,
        generator_version=dataset.generator_version,
        as_of="2020-01-01T00:00:00Z",
        batches=dataset.batches,
        batch_interval=dataset.batch_interval,
        tables=dataset.tables,
    )
    with pytest.raises(DataChaosError, match="outside the batch window"):
        engine.partition(dataset.tables["orders"], moved)


def test_run_id_depends_on_config_but_not_on_the_output_path(tmp_path: Path) -> None:
    first = engine.generate(RunConfig(dataset=SMALL), tmp_path / "a")
    second = engine.generate(RunConfig(dataset=SMALL), tmp_path / "elsewhere" / "b")
    other = engine.generate(RunConfig(seed=43, dataset=SMALL), tmp_path / "c")
    assert first.run.run_id == second.run.run_id
    assert first.run.run_id != other.run.run_id
    assert len(first.run.run_id) == 16


def test_created_at_is_recorded_only_in_run(tmp_path: Path) -> None:
    moment = "2000-01-02T03:04:05Z"
    engine.generate(RunConfig(dataset=SMALL), tmp_path / "out", created_at=moment)
    assert _manifest(tmp_path / "out")["run"]["created_at"] == moment
    data = json.dumps(_manifest(tmp_path / "out")["files"])
    assert moment not in data


def test_public_generate(tmp_path: Path) -> None:
    manifest = generate(tmp_path / "out", seed=1, customers=2, orders=3, batches=2)
    assert manifest.dataset.tables["orders"].rows_final == 3
    assert manifest.to_dict() == _manifest(tmp_path / "out")
