# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Project setup: packaging, ruff, mypy, pytest and a GitHub Actions matrix for Python 3.10–3.13.
- `datachaos --version`, and one-line `datachaos: error: …` messages with exit codes 0, 1 and 2.
  `--verbose` adds the stack trace.
- `datachaos generate`, and `datachaos.generate()` in Python: writes the clean `shop` dataset
  (`customers`, `orders`, `order_events`) in batches as CSV and/or JSONL, with a schema file per
  batch, an empty ledger and `chaos-manifest.json`. The manifest is described by
  `schema/chaos-manifest-1.0.schema.json`.
- Determinism: the same version and settings give byte-identical output, except for
  `run.created_at`. All randomness builds on `random.Random.random()` only.
- Output is written through a temp directory and renamed into place. Missing parent directories
  are created; a non-empty output directory or a temp directory left by an earlier run is an
  error, and nothing is deleted.
- `datachaos run`, with a YAML config or one `--scenario`, and `datachaos.run()`. `--force` replaces
  earlier output, but only when its manifest lists every file in the directory.
- `datachaos list-scenarios` and `datachaos describe`, with `--output json`, and
  `docs/scenarios.md` generated from the same specs.
- Running a scenario: every scenario is validated before any runs, each against the schema state
  the earlier ones leave behind, and a scenario's invariant is checked right after it runs. All
  changes go to `chaos-ledger.jsonl`.
- Scenarios `duplicate-business-key`, `null-critical-column`, `invalid-enum`,
  `orphaned-foreign-key`, `late-arriving-data`, `out-of-order-events` and `historical-update`. The manifest warns when NULL is written to a column the schema says is not nullable.

### Changed
- Orders are spread evenly over the time window, instead of piling up towards the end. Generator
  version 2 therefore gives other data than version 1. Late customers can be without orders.
- CI also runs on macOS and Windows with Python 3.12.
