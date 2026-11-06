# DataChaos

DataChaos will generate a synthetic, deterministic dataset delivered in batches, inject
controlled data quality faults, keep a ledger of exactly what changed, and check whether your
pipeline's dbt tests caught the faults.

**Status:** early development. The clean baseline dataset can be generated, all eight scenarios can
inject faults, and runs can be validated, reported on and landed batch by batch. Checking dbt results
is not implemented yet.

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
schema-drift            Changes the schema of a table from one batch on.
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
- `schema-drift` cannot be combined with another scenario on the same column, and a row always has
  the form of the batch it is in. `--combined` is refused when a table's schema varies.
- Everything is validated before any scenario runs, and all scenarios run before anything is
  written, so an error leaves no output.
- `--force` replaces earlier output, but only if its manifest lists every file in the directory.

## Check a run

`datachaos validate` has two levels, and they answer different questions:

| | `validate DIR` | `validate DIR --reproduce` |
|---|---|---|
| Question | Do the files on disk agree with the manifest and the ledger? | Would DataChaos write these files again? |
| Hashes, columns, schema changes | yes | yes |
| Every ledger line can be observed | yes | yes |
| Scenario invariants | no | yes, in a new run of the config |
| Needs the same DataChaos version | no | yes, or the files differ |

```console
$ datachaos validate chaos --reproduce
ok: chaos (files and ledger, and reproduced)
  ledger: 40 lines, 20 verified, 0 superseded, 20 unverifiable
datachaos: warning: 20 ledger lines cannot be verified: several rows share the key, or the key is NULL (--emit-row-ids makes them exact)
```

Exit code 1 means something is provably wrong, and exit code 2 that the directory is not DataChaos
output. `validate` never reports what it cannot prove. Row IDs are not in the data by default, so
ledger lines are checked through the key. A line whose key is shared by several rows or is NULL
counts as unverifiable, with a warning and exit code 0. With `--emit-row-ids` every line is checked
exactly. With `allow_overlap`, only the last line per row and column counts, and the earlier ones are
reported as superseded.

`datachaos report DIR` summarizes a run per scenario from the manifest and the ledger. It checks
nothing.

## Add a scenario to earlier output

```console
$ datachaos inject --input chaos --out more --scenario late-arriving-data --table orders --param count=5 --param delay_min=6h --param delay_max=1d
wrote more (run 7817a2a143c43c98)
  based on run e31a5622bdf493fe
  customers: 100 rows
  orders: 1020 rows
  order_events: 3300 rows
  scenario 0: duplicate-business-key on orders, 20 ledger lines
  scenario 1: null-critical-column on orders, 10 ledger lines
  scenario 2: invalid-enum on orders, 10 ledger lines
  scenario 3: late-arriving-data on orders, 9 ledger lines
datachaos: warning: scenarios[1]: orders.customer_id is not nullable in the schema, but 10 rows now hold NULL
```

`inject` does not read the data files, since row IDs are not on disk. It runs the config in the
manifest again and requires the same content, which needs the same DataChaos version. Then it adds
the scenario at the end and writes everything to a new directory. `run.based_on` in the new manifest
says which run it builds on.

## Land batches one at a time

```console
$ datachaos land chaos --upto-batch 2 --to landing
landed batches up to 2 in landing: 18 files copied, 0 already there
$ datachaos land chaos --upto-batch 3 --to landing
landed batches up to 3 in landing: 6 files copied, 18 already there
```

Data files go to `landing/<table>/` and schema files to `landing/schemas/<table>/`. `land` never
deletes or overwrites: an equal file is skipped, and a different one stops it with exit code 2 before
anything is copied.

## Machine-readable output

`--output json` on `generate`, `run`, `inject`, `validate`, `report`, `land`, `list-scenarios` and
`describe` writes one JSON document with `schema_version` and `tool_version` to stdout, and warnings
to stderr.

## Development

```bash
ruff check && ruff format --check && mypy && pytest
DATACHAOS_UPDATE_GOLDEN=1 pytest tests/golden   # only when a change in output is intended
python -m datachaos._docs                       # regenerate docs/scenarios.md
```

## License

MIT
