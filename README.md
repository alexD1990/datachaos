# DataChaos

DataChaos will generate a synthetic, deterministic dataset delivered in batches, inject
controlled data quality faults, keep a ledger of exactly what changed, and check whether your
pipeline's dbt tests caught the faults.

**Status:** early development. The clean baseline dataset can be generated; injecting faults and
checking dbt results are not implemented yet.

## Install

From a checkout, with Python 3.10 or newer:

```bash
pip install -e ".[dev]"
```

```console
$ datachaos --version
datachaos 0.1.0
```

## Generate the baseline

```console
$ datachaos generate --out shop --seed 1 --batches 3
wrote shop (run 641969d1c928467c)
  customers: 100 rows
  orders: 1000 rows
  order_events: 2665 rows
```

```text
order_id,customer_id,order_timestamp,updated_at,status,amount,currency,source_system,ingested_at
O000322,C000006,2025-12-29T01:54:51Z,2025-12-29T01:54:51Z,created,6730.62,SEK,webshop,2025-12-29T01:58:28Z
O000164,C000001,2025-12-29T03:14:28Z,2025-12-29T03:14:28Z,created,25552.01,NOK,pos,2025-12-29T03:33:11Z
```

The output directory must be missing or empty. Missing parent directories are created.

```text
shop/
  chaos-manifest.json
  chaos-ledger.jsonl                 # empty until faults are injected
  data/<table>/batch_0000.csv        # and/or .jsonl with --format jsonl|both
  schemas/<table>/batch_0000.json
  data/<table>.csv                   # only with --combined
```

### The `shop` dataset

- `customers` and `orders` hold the state at creation, so every order has the status `created`.
  Later statuses exist only in `order_events`. No row holds information from after its own
  `ingested_at`.
- A row belongs to the batch that its `ingested_at` falls in. A batch without rows still gets a
  file with only the header (CSV) or an empty file (JSONL).
- An order has 1 to 5 events. Events that would be ingested at or after `--as-of` are left out.

### Reading the files

- **NULL is an empty field in CSV.** The generator never writes an empty string, so this is
  unambiguous. JSONL uses `null`.
- UTF-8 without BOM, with `\n` line endings on every operating system.
- Decimals are strings with a fixed number of decimals, booleans are `true`/`false`, and
  timestamps are `YYYY-MM-DDTHH:MM:SSZ`.

### Determinism

The same version and settings give byte-identical data files, ledger and manifest, except for
`run.created_at` in the manifest.

## Development

```bash
ruff check && ruff format --check && mypy && pytest
DATACHAOS_UPDATE_GOLDEN=1 pytest tests/golden   # only when a change in output is intended
```

## License

MIT
