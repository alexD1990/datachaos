# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] - 2026-11-06

The first release: generate a dataset, inject faults, land it batch by batch, and check whether the
pipeline caught them.

### Added
- `datachaos generate`: writes the clean `shop` dataset (`customers`, `orders`, `order_events`) in
  batches as CSV and/or JSONL, with a schema file per batch, a ledger and `chaos-manifest.json`,
  described by `schema/chaos-manifest-1.0.schema.json`.
- `datachaos run`, with a YAML config or one `--scenario`. Every scenario is validated before any runs,
  each against the schema state the earlier ones leave behind, and its invariant is checked right
  after it runs. All changes go to `chaos-ledger.jsonl`. A scenario does not select rows whose key or
  group an earlier one has touched, unless `allow_overlap` is set. `--force` replaces earlier output,
  but only when its manifest lists every file in the directory.
- Eight scenarios: `duplicate-business-key`, `null-critical-column`, `invalid-enum`,
  `orphaned-foreign-key`, `late-arriving-data`, `out-of-order-events`, `historical-update` and
  `schema-drift`, which cannot be combined with another scenario on the same column.
- `datachaos list-scenarios` and `datachaos describe`, and `docs/scenarios.md` generated from the same
  specs.
- `datachaos validate`. Without flags it checks what can be decided from disk: hashes, columns per batch
  schema, schema changes, and that each ledger line can be observed. It never reports what it cannot
  prove: without `--emit-row-ids`, a line whose key is shared or NULL counts as unverifiable, with a
  warning and exit code 0. `--reproduce` also runs the config in the manifest again, which checks the
  scenario invariants.
- `datachaos report`: a summary per scenario, from the manifest and the ledger alone.
- `datachaos inject`: adds a scenario to earlier output, in a new directory, by running its config
  again. It first checks that this gives the content of the input, and the new manifest records which
  run it builds on in `run.based_on`.
- `datachaos land`: copies batches up to one, with their schema files, to a landing directory. It never
  deletes or overwrites: an equal file is skipped, and a different one stops it before anything is
  copied.
- `datachaos check-dbt`: compares one or more dbt `run_results.json` files with the expectations in a
  config. A node failed if it has status `fail` or `error` in any file, and failures nobody expected are
  reported. A run without scenarios expects every node to pass.
- `--output json` on every command: stdout holds only one JSON document with `schema_version` and
  `tool_version`, and warnings go to stderr. Exit codes 0, 1 and 2, and one-line
  `datachaos: error: ...` messages, with the stack trace behind `--verbose`.
- `datachaos.generate()`, `datachaos.run()` and `datachaos.validate()` in Python.
- Determinism: the same version and config give byte-identical output, except for `run.created_at`.
  All randomness builds on `random.Random.random()` only.
- Output is written through a temp directory and renamed into place. Missing parent directories are
  created. A non-empty output directory or a temp directory left by an earlier run is an error, and
  nothing is deleted.
- `examples/dbt_shop`: a dbt-duckdb project with vulnerable and robust incremental models, and
  `run_demo.py`, which runs the whole loop for a baseline and every scenario. It needs a clone of the
  repository and `pip install -e ".[dev,dbt]"`.
- CI on Python 3.10 to 3.13 on Ubuntu and Python 3.12 on macOS and Windows, a job with the demo against
  real dbt, a job that uses the wheel in a clean venv, and `publish.yml` with trusted publishing.
