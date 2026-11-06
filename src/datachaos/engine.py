"""Ties it together: builds the dataset, writes it and describes it in the manifest."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path

from datachaos._version import __version__
from datachaos.config import RunConfig, ScenarioConfig, validate_config
from datachaos.datasets import DATASETS
from datachaos.errors import DataChaosError, FieldError
from datachaos.io.csv_writer import write_csv
from datachaos.io.jsonl_writer import write_json, write_jsonl, write_ledger
from datachaos.io.output_dir import atomic_output, sha256_file
from datachaos.model.dataset import Dataset
from datachaos.model.ledger import LedgerEntry
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
from datachaos.model.table import Batch, Row, Table, Value
from datachaos.rng import Rng, derive_seed
from datachaos.scenarios import SCENARIOS
from datachaos.scenarios.base import (
    ResolveContext,
    ResolvedParams,
    Scenario,
    ScenarioContext,
    ScenarioSpec,
    did_you_mean,
    key_of,
)
from datachaos.scenarios.catalog import render_markdown, render_text, spec_to_dict
from datachaos.timeutil import (
    batch_index,
    parse_duration,
    parse_timestamp,
    utc_now,
    window_start,
)

ROW_ID_COLUMN = Column("_chaos_rid", ColumnType.STRING, False)
_WRITERS = {"csv": write_csv, "jsonl": write_jsonl}


@dataclass(frozen=True)
class _Planned:
    index: int
    scenario: Scenario
    table: str
    params: ResolvedParams


@dataclass(frozen=True)
class _Applied:
    dataset: Dataset
    ledger: tuple[LedgerEntry, ...]
    records: tuple[dict[str, object], ...]
    warnings: tuple[str, ...]


def generate(config: RunConfig, out: Path, *, created_at: str | None = None) -> Manifest:
    """Writes the baseline dataset to `out`. Nothing is written unless the config is valid."""
    return run(config, out, created_at=created_at)


def run(
    config: RunConfig, out: Path, *, force: bool = False, created_at: str | None = None
) -> Manifest:
    """Builds the dataset, applies the scenarios and writes it all to `out`.

    Every scenario is resolved before any runs, and all of them run before anything is written,
    so an error leaves no output.
    """
    validate_config(config)
    baseline = _build_dataset(config)
    planned = resolve_scenarios(config, baseline)
    applied = _apply_scenarios(config, baseline, planned)
    resolved = replace(
        config,
        scenarios=tuple(
            ScenarioConfig(item.scenario.spec.name, item.table, item.params) for item in planned
        ),
    )
    run_id = compute_run_id(resolved)
    with atomic_output(out, run_id, force=force) as root:
        files = [_write_ledger(root, applied.ledger), *_write_tables(root, applied.dataset, config)]
        files.sort(key=lambda entry: entry.path)
        manifest = Manifest(
            tool_version=__version__,
            run=RunInfo(run_id, created_at or utc_now(), config.seed, resolved.to_dict()),
            dataset=DatasetInfo(
                name=baseline.name,
                generator_version=baseline.generator_version,
                as_of=baseline.as_of,
                batches=baseline.batches,
                batch_interval=baseline.batch_interval,
                tables={
                    name: TableCounts(len(table.rows), len(applied.dataset.tables[name].rows))
                    for name, table in baseline.tables.items()
                },
            ),
            scenarios=applied.records,
            files=tuple(files),
            content_sha256=compute_content_sha256(files),
            warnings=applied.warnings,
        )
        write_json(root / MANIFEST_FILE, manifest.to_dict())
    return manifest


def resolve_scenarios(config: RunConfig, baseline: Dataset) -> tuple[_Planned, ...]:
    """Resolves every scenario, each against the schema state the earlier ones leave behind."""
    ctx = ResolveContext(
        dataset=baseline,
        schemas={name: table.schema_by_batch for name, table in baseline.tables.items()},
    )
    planned = []
    for index, item in enumerate(config.scenarios):
        if item.name not in SCENARIOS:
            raise DataChaosError(
                f"scenarios[{index}].name: unknown scenario {item.name!r}"
                f"{did_you_mean(item.name, SCENARIOS)}"
            )
        scenario = SCENARIOS[item.name]
        try:
            params = scenario.resolve(ctx, item.table, item.params)
            ctx = replace(ctx, schemas=scenario.schemas_after(ctx, item.table, params))
        except FieldError as exc:
            raise DataChaosError(f"scenarios[{index}].{exc.field}: {exc.message}") from exc
        planned.append(_Planned(index, scenario, item.table, params))
    return tuple(planned)


def _apply_scenarios(
    config: RunConfig, baseline: Dataset, planned: tuple[_Planned, ...]
) -> _Applied:
    dataset = baseline
    ledger: list[LedgerEntry] = []
    records: list[dict[str, object]] = []
    warnings: list[str] = []
    for item in planned:
        name = item.scenario.spec.name
        seed = derive_seed(config.seed, f"scenario:{item.index}:{name}")
        ctx = ScenarioContext(
            dataset=dataset,
            table=item.table,
            params=item.params,
            rng=Rng(seed),
            index=item.index,
            eligible=_eligibility(dataset, item, ledger),
            eligible_in=_eligibility_in(dataset, item, ledger),
        )
        try:
            outcome = item.scenario.apply(ctx)
        except FieldError as exc:
            raise DataChaosError(f"scenarios[{item.index}].{exc.field}: {exc.message}") from exc
        except DataChaosError as exc:
            raise DataChaosError(f"scenarios[{item.index}]: {exc}") from exc
        problems = item.scenario.check(ctx, outcome)
        if problems:
            raise DataChaosError(
                f"internal error: scenario {name} broke its invariant: {problems[0]}"
            )
        dataset = outcome.dataset
        ledger.extend(outcome.ledger)
        warnings.extend(outcome.warnings)
        records.append(
            {
                "index": item.index,
                "name": name,
                "version": item.scenario.spec.version,
                "table": item.table,
                "seed": seed,
                "params": dict(item.params),
                "stats": outcome.stats.to_dict(),
                "ledger_entries": len(outcome.ledger),
            }
        )
    ledger.sort(
        key=lambda entry: (entry.scenario_index, entry.table, entry.rid, entry.column or "")
    )
    return _Applied(dataset, tuple(ledger), tuple(records), tuple(warnings))


def _eligibility(
    dataset: Dataset, item: _Planned, ledger: list[LedgerEntry]
) -> Callable[[Row], bool]:
    return _eligibility_in(dataset, item, ledger)(item.table)


def _eligibility_in(
    dataset: Dataset, item: _Planned, ledger: list[LedgerEntry]
) -> Callable[[str], Callable[[Row], bool]]:
    def for_table(table: str) -> Callable[[Row], bool]:
        if item.params.get("allow_overlap"):
            return lambda row: True
        return _not_touched(dataset, table, ledger)

    return for_table


def _not_touched(dataset: Dataset, table: str, ledger: list[LedgerEntry]) -> Callable[[Row], bool]:
    """A row is not a candidate if an earlier scenario touched it, its key or its group."""
    earlier = [entry for entry in ledger if entry.table == table]
    rids = {entry.rid for entry in earlier} | {entry.source_rid for entry in earlier}
    keys = {_key_identity(entry.key) for entry in earlier}
    touched_groups = [entry.group for entry in earlier if entry.group is not None]
    groups = {_key_identity(group) for group in touched_groups}
    group_columns = sorted({tuple(sorted(group)) for group in touched_groups})
    schema = dataset.tables[table].schema_by_batch[0]

    def eligible(row: Row) -> bool:
        if row.rid in rids or _key_identity(key_of(row, schema)) in keys:
            return False
        return not any(
            _key_identity({column: row.values.get(column) for column in columns}) in groups
            for columns in group_columns
        )

    return eligible


def _key_identity(key: Mapping[str, Value]) -> tuple[tuple[str, Value], ...]:
    return tuple(sorted(key.items(), key=lambda item: item[0]))


def scenario_list() -> list[dict[str, object]]:
    return [spec_to_dict(spec) for spec in _specs()]


def scenario_details(name: str) -> dict[str, object]:
    return spec_to_dict(_spec(name))


def scenario_text(name: str) -> str:
    return render_text(_spec(name))


def scenarios_markdown() -> str:
    return render_markdown(_specs())


def _specs() -> list[ScenarioSpec]:
    return [SCENARIOS[name].spec for name in sorted(SCENARIOS)]


def _spec(name: str) -> ScenarioSpec:
    if name not in SCENARIOS:
        raise DataChaosError(f"unknown scenario {name!r}{did_you_mean(name, SCENARIOS)}")
    return SCENARIOS[name].spec


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


def _write_ledger(root: Path, entries: Sequence[LedgerEntry]) -> FileEntry:
    rows = write_ledger(root / LEDGER_FILE, entries)
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
