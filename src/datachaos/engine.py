"""Ties it together: builds the dataset, writes it and describes it in the manifest."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

from datachaos._version import __version__
from datachaos.config import RunConfig, validate_config
from datachaos.datasets import DATASETS
from datachaos.errors import DataChaosError
from datachaos.io.csv_writer import write_csv
from datachaos.io.jsonl_writer import write_json, write_jsonl, write_ledger
from datachaos.io.output_dir import atomic_output, sha256_file
from datachaos.model.dataset import Dataset
from datachaos.model.manifest import (
    LEDGER_FILE,
    MANIFEST_FILE,
    SCHEMA_VERSION,
    DatasetInfo,
    FileEntry,
    Manifest,
    RunInfo,
    TableCounts,
    compute_content_sha256,
)
from datachaos.model.schema import Column, ColumnType, Role, TableSchema
from datachaos.model.table import Batch, Row, Table
from datachaos.timeutil import (
    batch_index,
    parse_duration,
    parse_timestamp,
    utc_now,
    window_start,
)

ROW_ID_COLUMN = Column("_chaos_rid", ColumnType.STRING, False)
_WRITERS = {"csv": write_csv, "jsonl": write_jsonl}


def generate(config: RunConfig, out: Path, *, created_at: str | None = None) -> Manifest:
    """Writes the baseline dataset to `out`. Nothing is written unless the config is valid."""
    validate_config(config)
    dataset = _build_dataset(config)
    run_id = compute_run_id(config)
    with atomic_output(out, run_id) as root:
        files = [_write_empty_ledger(root), *_write_tables(root, dataset, config)]
        files.sort(key=lambda entry: entry.path)
        manifest = Manifest(
            tool_version=__version__,
            run=RunInfo(run_id, created_at or utc_now(), config.seed, config.to_dict()),
            dataset=DatasetInfo(
                name=dataset.name,
                generator_version=dataset.generator_version,
                as_of=dataset.as_of,
                batches=dataset.batches,
                batch_interval=dataset.batch_interval,
                tables={
                    name: TableCounts(len(table.rows), len(table.rows))
                    for name, table in dataset.tables.items()
                },
            ),
            scenarios=(),
            files=tuple(files),
            content_sha256=compute_content_sha256(files),
        )
        write_json(root / MANIFEST_FILE, manifest.to_dict())
    return manifest


def compute_run_id(config: RunConfig) -> str:
    canonical = json.dumps(
        {"config": config.to_dict(), "tool_version": __version__},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def partition(table: Table, dataset: Dataset) -> tuple[Batch, ...]:
    """Divides a table into its batches. Batch membership follows from ingested_at alone."""
    interval = parse_duration(dataset.batch_interval)
    start = window_start(parse_timestamp(dataset.as_of), dataset.batches, interval)
    by_batch: list[list[Row]] = [[] for _ in range(dataset.batches)]
    column = table.schema_by_batch[0].column_with_role(Role.INGESTION_TIME).name
    for row in table.rows:
        index = batch_index(parse_timestamp(str(row.values[column])), start, interval)
        if not 0 <= index < dataset.batches:
            raise DataChaosError(f"internal error: row {row.rid} is outside the batch window")
        by_batch[index].append(row)
    return tuple(
        Batch(index, table.schema_by_batch[index], tuple(rows))
        for index, rows in enumerate(by_batch)
    )


def _build_dataset(config: RunConfig) -> Dataset:
    settings = config.dataset
    return DATASETS[settings.name](
        seed=config.seed,
        as_of=settings.as_of,
        customers=settings.customers,
        orders=settings.orders,
        batches=settings.batches,
        batch_interval=settings.batch_interval,
    )


def _write_empty_ledger(root: Path) -> FileEntry:
    rows = write_ledger(root / LEDGER_FILE, ())
    return FileEntry(LEDGER_FILE, sha256_file(root / LEDGER_FILE), rows)


def _write_tables(root: Path, dataset: Dataset, config: RunConfig) -> list[FileEntry]:
    output = config.output
    files = []
    for name, table in dataset.tables.items():
        batches = partition(table, dataset)
        if output.emit_row_ids:
            batches = tuple(_with_row_ids(batch) for batch in batches)
        (root / "data" / name).mkdir(parents=True)
        (root / "schemas" / name).mkdir(parents=True)
        for batch in batches:
            files.append(_write_schema(root, name, batch))
            for file_format in output.formats:
                path = f"data/{name}/batch_{batch.index:04d}.{file_format}"
                files.append(_write_rows(root, path, file_format, batch.schema, batch.rows))
        if output.combined:
            files.extend(_write_combined(root, name, batches, output.formats))
    return files


def _write_schema(root: Path, table: str, batch: Batch) -> FileEntry:
    path = f"schemas/{table}/batch_{batch.index:04d}.json"
    document = batch.schema.to_dict(
        schema_version=SCHEMA_VERSION, tool_version=__version__, batch=batch.index
    )
    write_json(root / path, document)
    return FileEntry(path, sha256_file(root / path))


def _write_rows(
    root: Path, path: str, file_format: str, schema: TableSchema, rows: Sequence[Row]
) -> FileEntry:
    count = _WRITERS[file_format](root / path, schema, rows)
    return FileEntry(path, sha256_file(root / path), count)


def _write_combined(
    root: Path, table: str, batches: tuple[Batch, ...], formats: tuple[str, ...]
) -> list[FileEntry]:
    schema = batches[0].schema
    if any(batch.schema != schema for batch in batches):
        raise DataChaosError(f"cannot combine {table}: its schema differs between batches")
    rows = [row for batch in batches for row in batch.rows]
    return [
        _write_rows(root, f"data/{table}.{file_format}", file_format, schema, rows)
        for file_format in formats
    ]


def _with_row_ids(batch: Batch) -> Batch:
    return replace(
        batch,
        schema=replace(batch.schema, columns=(*batch.schema.columns, ROW_ID_COLUMN)),
        rows=tuple(
            replace(row, values={**row.values, ROW_ID_COLUMN.name: row.rid}) for row in batch.rows
        ),
    )
