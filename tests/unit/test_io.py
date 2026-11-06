from __future__ import annotations

from pathlib import Path

import pytest

from datachaos.errors import DataChaosError
from datachaos.io.csv_writer import write_csv
from datachaos.io.jsonl_writer import write_json, write_jsonl
from datachaos.io.reader import read_csv, read_json, read_jsonl, read_schema
from datachaos.model.schema import Column, ColumnType, Role, TableSchema
from datachaos.model.table import Row

SCHEMA = TableSchema(
    name="t",
    kind="entity",
    columns=(
        Column("id", ColumnType.STRING, False, Role.KEY),
        Column("city", ColumnType.STRING, True),
        Column("count", ColumnType.INTEGER, True),
        Column("amount", ColumnType.DECIMAL, True, scale=2),
        Column("active", ColumnType.BOOLEAN, True),
        Column("day", ColumnType.DATE, True),
        Column("at", ColumnType.TIMESTAMP, True),
    ),
)
ROWS = (
    Row(
        "r1",
        {
            "id": "a",
            "city": "Ålesund",
            "count": 3,
            "amount": "12.50",
            "active": True,
            "day": "2026-01-05",
            "at": "2026-01-05T14:05:00Z",
        },
    ),
    Row(
        "r2",
        {
            "id": "b",
            "city": 'komma, og "sitat"',
            "count": None,
            "amount": None,
            "active": False,
            "day": None,
            "at": None,
        },
    ),
    Row(
        "r3",
        {
            "id": "c",
            "city": "linje\nskift\r\nslutt",
            "count": 0,
            "amount": "0.00",
            "active": None,
            "day": None,
            "at": None,
        },
    ),
)


def test_csv_bytes_are_exact(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    assert write_csv(path, SCHEMA, ROWS[:2]) == 2
    assert (
        path.read_bytes()
        == (
            "id,city,count,amount,active,day,at\n"
            "a,Ålesund,3,12.50,true,2026-01-05,2026-01-05T14:05:00Z\n"
            'b,"komma, og ""sitat""",,,false,,\n'
        ).encode()
    )


def test_jsonl_bytes_are_exact(tmp_path: Path) -> None:
    path = tmp_path / "t.jsonl"
    assert write_jsonl(path, SCHEMA, ROWS[:2]) == 2
    assert (
        path.read_bytes()
        == (
            '{"id":"a","city":"Ålesund","count":3,"amount":"12.50","active":true,'
            '"day":"2026-01-05","at":"2026-01-05T14:05:00Z"}\n'
            '{"id":"b","city":"komma, og \\"sitat\\"","count":null,"amount":null,"active":false,'
            '"day":null,"at":null}\n'
        ).encode()
    )


def test_no_byte_order_mark_and_no_carriage_returns_outside_values(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    write_csv(path, SCHEMA, ROWS[:2])
    data = path.read_bytes()
    assert not data.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in data


@pytest.mark.parametrize(
    ("writer", "reader", "name"),
    [(write_csv, read_csv, "t.csv"), (write_jsonl, read_jsonl, "t.jsonl")],
)
def test_round_trip(tmp_path: Path, writer, reader, name: str) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / name
    writer(path, SCHEMA, ROWS)
    assert reader(path, SCHEMA) == [dict(row.values) for row in ROWS]


def test_empty_batch_is_a_header_only_csv_and_an_empty_jsonl(tmp_path: Path) -> None:
    assert write_csv(tmp_path / "t.csv", SCHEMA, ()) == 0
    assert write_jsonl(tmp_path / "t.jsonl", SCHEMA, ()) == 0
    assert (tmp_path / "t.csv").read_bytes() == b"id,city,count,amount,active,day,at\n"
    assert (tmp_path / "t.jsonl").read_bytes() == b""
    assert read_csv(tmp_path / "t.csv", SCHEMA) == []
    assert read_jsonl(tmp_path / "t.jsonl", SCHEMA) == []


def test_json_document_keeps_unicode_and_ends_with_newline(tmp_path: Path) -> None:
    path = tmp_path / "d.json"
    write_json(path, {"by": "Ålesund"})
    assert path.read_bytes() == '{\n  "by": "Ålesund"\n}\n'.encode()
    assert read_json(path) == {"by": "Ålesund"}


def test_schema_file_is_read_back(tmp_path: Path) -> None:
    path = tmp_path / "s.json"
    write_json(path, SCHEMA.to_dict(schema_version="1.0", tool_version="0", batch=0))
    assert read_schema(path) == SCHEMA


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"wrong,header\n", "does not match schema"),
        (b"", "does not match schema"),
        (b"id,city,count,amount,active,day,at\na,b\n", "expected 7 fields, got 2"),
        (b"id,city,count,amount,active,day,at\na,,x,,,,\n", "'x' is not an integer"),
        (b"id,city,count,amount,active,day,at\na,,,,yes,,\n", "'yes' is not a boolean"),
        (b"id,city,count,amount,active,day,at\n\xff,,,,,,\n", "cannot read"),
    ],
)
def test_bad_csv_is_a_datachaos_error(tmp_path: Path, content: bytes, message: str) -> None:
    path = tmp_path / "t.csv"
    path.write_bytes(content)
    with pytest.raises(DataChaosError, match=message):
        read_csv(path, SCHEMA)


@pytest.mark.parametrize(
    ("content", "message"), [(b'{"id":"a"}\n', "keys do not match"), (b"{nope\n", "cannot read")]
)
def test_bad_jsonl_is_a_datachaos_error(tmp_path: Path, content: bytes, message: str) -> None:
    path = tmp_path / "t.jsonl"
    path.write_bytes(content)
    with pytest.raises(DataChaosError, match=message):
        read_jsonl(path, SCHEMA)


def test_bad_or_missing_json_is_a_datachaos_error(tmp_path: Path) -> None:
    with pytest.raises(DataChaosError, match="cannot read"):
        read_json(tmp_path / "missing.json")
    (tmp_path / "list.json").write_bytes(b"[]")
    with pytest.raises(DataChaosError, match="expected a JSON object"):
        read_json(tmp_path / "list.json")
    (tmp_path / "bad.json").write_bytes(b"{}")
    with pytest.raises(DataChaosError, match="invalid schema file"):
        read_schema(tmp_path / "bad.json")
