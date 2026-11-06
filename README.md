# DataChaos

DataChaos will generate a synthetic, deterministic dataset delivered in batches, inject
controlled data quality faults, keep a ledger of exactly what changed, and check whether your
pipeline's dbt tests caught the faults.

**Status:** early development. The clean baseline dataset can be generated and seven of the eight
planned scenarios can inject faults; checking dbt results is not implemented yet.

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
  order_events: 3216 rows
```

```text
order_id,customer_id,order_timestamp,updated_at,status,amount,currency,source_system,ingested_at
O000150,C000001,2025-12-29T00:01:33Z,2025-12-29T00:01:33Z,created,34752.88,NOK,pos,2025-12-29T00:02:04Z
O000975,C000001,2025-12-29T00:00:23Z,2025-12-29T00:00:23Z,created,32987.31,NOK,webshop,2025-12-29T00:15:31Z
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

## Inject faults

```console
$ datachaos list-scenarios
duplicate-business-key  Adds a second row with the same business key.
historical-update       Adds a new version of old rows in a later batch, with a new updated_at.
invalid-enum            Replaces a category with a value outside the allowed values.
late-arriving-data      Delays ingested_at, so that rows land in a later batch than their event time.
null-critical-column    Sets a column to NULL, whether or not the schema allows it.
orphaned-foreign-key    Makes foreign keys point at a parent that does not exist.
out-of-order-events     Holds back the next to last event of a group, so that it lands after the last.
```

`datachaos describe <scenario>` shows what a scenario detects and its parameters, and
[docs/scenarios.md](docs/scenarios.md) lists them all.

Several scenarios go in a config file, `chaos.yml`:

```yaml
version: 1
seed: 42
dataset:
  as_of: "2026-01-08T00:00:00Z"
  customers: 100
  orders: 1000
  batches: 7
  batch_interval: 1d
scenarios:
  - name: duplicate-business-key
    table: orders
    params: {rate: 0.02}
  - name: null-critical-column
    table: orders
    params: {column: customer_id, count: 10}
  - name: invalid-enum
    table: orders
    params: {column: currency, rate: 0.01, variant: case_change}
```

```console
$ datachaos run --config chaos.yml --out chaos
wrote chaos (run e31a5622bdf493fe)
  customers: 100 rows
  orders: 1020 rows
  order_events: 3300 rows
  scenario 0: duplicate-business-key on orders, 20 ledger lines
  scenario 1: null-critical-column on orders, 10 ledger lines
  scenario 2: invalid-enum on orders, 10 ledger lines
datachaos: warning: scenarios[1]: orders.customer_id is not nullable in the schema, but 10 rows now hold NULL
```

`--out` is always required. One scenario can also be given directly:

```bash
datachaos run --out chaos --scenario invalid-enum --table orders --param column=status --param count=5
```

`chaos-ledger.jsonl` holds one line per change:

```text
{"scenario_index":0,"scenario":"duplicate-business-key","table":"orders","action":"insert_row","rid":"s0000-0000001","source_rid":"o000181","key":{"order_id":"O000181"},"batch":3,"column":null,"before":null,"after":null}
{"scenario_index":0,"scenario":"duplicate-business-key","table":"orders","action":"insert_row","rid":"s0000-0000002","source_rid":"o000190","key":{"order_id":"O000190"},"batch":1,"column":null,"before":null,"after":null}
```

- By default a scenario does not select a row whose key or group an earlier scenario has touched.
  `allow_overlap: true` turns that off.
- Everything is validated before any scenario runs, and all scenarios run before anything is
  written, so an error leaves no output.
- `--force` replaces earlier output, but only if its manifest lists every file in the directory.

## Development

```bash
ruff check && ruff format --check && mypy && pytest
DATACHAOS_UPDATE_GOLDEN=1 pytest tests/golden   # only when a change in output is intended
python -m datachaos._docs                       # regenerate docs/scenarios.md
```

## License

MIT
