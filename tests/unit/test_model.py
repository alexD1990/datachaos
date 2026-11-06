from __future__ import annotations

import hashlib
import json

from datachaos.model.ledger import LedgerEntry
from datachaos.model.manifest import FileEntry, compute_content_sha256
from datachaos.model.schema import Column, ColumnType, Role, TableSchema


def test_ledger_entry_matches_the_documented_lines() -> None:
    insert = LedgerEntry(
        scenario_index=0,
        scenario="duplicate-business-key",
        table="orders",
        action="insert_row",
        rid="s0-1",
        source_rid="o000417",
        key={"order_id": "O000417"},
        batch=3,
    )
    assert json.dumps(insert.to_dict(), separators=(",", ":")) == (
        '{"scenario_index":0,"scenario":"duplicate-business-key","table":"orders",'
        '"action":"insert_row","rid":"s0-1","source_rid":"o000417",'
        '"key":{"order_id":"O000417"},"batch":3,"column":null,"before":null,"after":null}'
    )
    change = LedgerEntry(
        scenario_index=1,
        scenario="null-critical-column",
        table="orders",
        action="set_value",
        rid="o000052",
        key={"order_id": "O000052"},
        batch=0,
        column="customer_id",
        before="C000031",
    )
    assert json.dumps(change.to_dict(), separators=(",", ":")) == (
        '{"scenario_index":1,"scenario":"null-critical-column","table":"orders",'
        '"action":"set_value","rid":"o000052","key":{"order_id":"O000052"},"batch":0,'
        '"column":"customer_id","before":"C000031","after":null}'
    )


def test_schema_round_trips_through_its_file_format() -> None:
    schema = TableSchema(
        name="orders",
        kind="entity",
        columns=(
            Column("id", ColumnType.STRING, False, Role.KEY),
            Column("parent", ColumnType.STRING, True, Role.FOREIGN_KEY, references="p.id"),
            Column("state", ColumnType.STRING, False, Role.ENUM, allowed_values=("a", "b")),
            Column("amount", ColumnType.DECIMAL, False, scale=2),
        ),
    )
    document = schema.to_dict(schema_version="1.0", tool_version="9.9", batch=4)
    assert document["schema_version"] == "1.0"
    assert document["tool_version"] == "9.9"
    assert document["batch"] == 4
    assert TableSchema.from_dict(json.loads(json.dumps(document))) == schema


def test_content_sha256_sorts_on_path_not_on_the_line() -> None:
    # The hash of z sorts first, so sorting whole lines would give the opposite order.
    first = FileEntry("data/a.csv", "f" * 64)
    second = FileEntry("data/z.csv", "0" * 64)
    expected = f"{'f' * 64}  data/a.csv\n{'0' * 64}  data/z.csv\n"
    digest = hashlib.sha256(expected.encode("utf-8")).hexdigest()
    assert compute_content_sha256([second, first]) == digest
    assert compute_content_sha256([first, second]) == digest


def test_content_sha256_covers_the_ledger_and_data_but_not_schemas() -> None:
    ledger = FileEntry("chaos-ledger.jsonl", "1" * 64, 0)
    data = FileEntry("data/a/batch_0000.csv", "2" * 64, 3)
    schema = FileEntry("schemas/a/batch_0000.json", "3" * 64)
    expected = f"{'1' * 64}  chaos-ledger.jsonl\n{'2' * 64}  data/a/batch_0000.csv\n"
    assert (
        compute_content_sha256([schema, data, ledger])
        == hashlib.sha256(expected.encode("utf-8")).hexdigest()
    )
