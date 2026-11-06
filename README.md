# DataChaos

Do your dbt tests catch what they are supposed to catch? DataChaos generates a synthetic, deterministic
dataset delivered in batches, injects controlled data quality faults, keeps a ledger of exactly what
it changed, and checks whether your pipeline noticed.

It works on local files. There is no warehouse, no server and no network, and the same config always
gives the same bytes.

## Quickstart

With Python 3.10 or newer:

```console
$ pip install datachaos
$ datachaos generate --out shop --seed 1 --batches 3
wrote shop (run 641969d1c928467c)
  customers: 100 rows
  orders: 1000 rows
  order_events: 3216 rows
$ datachaos run --out chaos --seed 1 --batches 3 --scenario duplicate-business-key --table orders --param count=5
wrote chaos (run c71c4d1d42037444)
  customers: 100 rows
  orders: 1005 rows
  order_events: 3216 rows
  scenario 0: duplicate-business-key on orders, 5 ledger lines
$ datachaos validate chaos --reproduce
datachaos: warning: 5 ledger lines cannot be verified: several rows share the key, or the key is NULL (--emit-row-ids makes them exact)
ok: chaos (files and ledger, and reproduced)
  ledger: 5 lines, 0 verified, 0 superseded, 5 unverifiable
$ datachaos report chaos
run c71c4d1d42037444: seed 1, as of 2026-01-01T00:00:00Z, 3 batches
  customers: 100 -> 100 rows
  orders: 1000 -> 1005 rows
  order_events: 3216 -> 3216 rows
scenario 0: duplicate-business-key on orders (version 1)
  ledger: insert_row 5
  example keys: O000197, O000362, O000754, O000772, O000796
5 ledger lines. Not checked; use validate for that.
```

- `generate` writes a clean dataset, your control group: `customers`, `orders` and `order_events`, one
  file per batch.
- `run` writes the same dataset with a fault in it, here five duplicated orders. Every change is a line
  in `chaos/chaos-ledger.jsonl`.
- `validate` checks the files against the manifest and the ledger, and `--reproduce` runs it all again.
  The warning is expected: see [Checking a run](#checking-a-run).
- `report` summarizes what was done.

Now point your pipeline at `chaos/data/` and see what your tests say. To do that one batch at a time
and compare the result with what you expect, see [The whole loop](#the-whole-loop).

## Scenarios

| Scenario | What it reveals | Typical test |
|---|---|---|
| `duplicate-business-key` | Broken grain, and staging models that do not deduplicate. | unique |
| `historical-update` | Incremental models that only see new keys, or that filter on event time instead of updated_at. | singular test comparing the incremental model with a full refresh |
| `invalid-enum` | Unknown categories that fall out of CASE expressions and mappings. | accepted_values |
| `late-arriving-data` | Incremental models with a watermark on event time, which never pick up late rows. | singular test comparing the incremental model with a full refresh |
| `null-critical-column` | Missing not_null tests, and joins that lose rows on NULL. | not_null |
| `orphaned-foreign-key` | Inner joins that lose rows, and missing referential integrity. | relationships |
| `out-of-order-events` | Latest-event-wins logic that sorts on ingested_at instead of event time, or that assumes events arrive in order. | singular test that the current status comes from the latest event time |
| `schema-drift` | Pipelines that fail or silently lose data when a schema changes. | none; often shows up as a model with status error |

`datachaos describe <scenario>` shows the parameters of a scenario, and they are all in
[docs/scenarios.md](https://github.com/alexD1990/datachaos/blob/master/docs/scenarios.md).

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

## The whole loop

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

By default a scenario does not select a row whose key or group an earlier scenario has touched, and
`allow_overlap: true` turns that off. Everything is validated before any scenario runs, and all of them
run before anything is written, so an error leaves no output.

```console
$ datachaos run --config chaos.yml --out full
datachaos: warning: scenarios[1]: orders.customer_id is not nullable in the schema, but 10 rows now hold NULL
wrote full (run e31a5622bdf493fe)
  customers: 100 rows
  orders: 1020 rows
  order_events: 3300 rows
  scenario 0: duplicate-business-key on orders, 20 ledger lines
  scenario 1: null-critical-column on orders, 10 ledger lines
  scenario 2: invalid-enum on orders, 10 ledger lines
```

Land the batches one at a time, as they would arrive, and run your pipeline after each:

```console
$ datachaos land full --upto-batch 0 --to landing
landed batches up to 0 in landing: 6 files copied, 0 already there
$ datachaos land full --upto-batch 1 --to landing
landed batches up to 1 in landing: 6 files copied, 6 already there
```

Data files go to `landing/<table>/` and schema files to `landing/schemas/<table>/`. `land` never deletes
or overwrites: an equal file is skipped, and a different one stops it before anything is copied.

Then say what should have been caught, in the same config:

```yaml
expectations:
  - scenario: duplicate-business-key
    failing_tests: ["test.my_project.assert_orders_unique*"]
```

Keep `target/run_results.json` after each dbt call, and compare:

```bash
datachaos check-dbt --run full --config chaos.yml \
  --run-results run_batch_0.json --run-results run_batch_1.json --run-results test.json
```

The results are merged: a node failed if it has status `fail` or `error` in any file. This matters,
because after a model error in `dbt run`, `dbt test` runs on the old table and may pass. Exit code 1
means that an expected failure did not happen, and a pattern that matches no node is a mistake in the
config, with exit code 2. Other failures are reported as unexpected. A run without scenarios expects
every node to pass.

## The dbt demo

`examples/dbt_shop` is a dbt-duckdb project with vulnerable and robust incremental models, and a script
that runs the whole loop for a baseline and every scenario. It needs a clone of the repository, since
the example is not part of the package:

```console
$ git clone https://github.com/alexD1990/datachaos
$ cd datachaos
$ pip install -e ".[dev,dbt]"
$ python examples/dbt_shop/run_demo.py
config                    result  caught by
baseline                  ok      -
duplicate_business_key    ok      assert_raw_orders_unique_per_version
historical_update         ok      assert_daily_orders_event_watermark_matches_full
invalid_enum              ok      accepted_values_stg_orders_currency
late_customers            ok      assert_fct_orders_incremental_matches_full
late_orders               ok      assert_daily_orders_event_watermark_matches_full
null_critical_column      ok      not_null_stg_orders_customer_id
orphaned_deleted_parent   ok      relationships_stg_orders_customer_id
orphaned_nonexistent      ok      relationships_stg_orders_customer_id
out_of_order_events       ok      assert_order_status_by_ingestion_matches_event_time
schema_drift_change_type  ok      model stg_orders, relationships_stg_order_events_order_id
schema_drift_rename       ok      not_null_stg_orders_source_system
```

It runs `dbt run` after each `land --upto-batch k` and `dbt test` once at the end, since `dbt build` per
batch would skip models downstream of a failing test and leave the incremental models in another state.
A config passes only if what it expects was caught and nothing else failed, so a robust model failing
under a scenario fails the demo.

Sources read everything as text, with `all_varchar=true` and `union_by_name=true`, and staging casts to
the right types. That decides what a schema change looks like there:

| `schema-drift` | What happens in this pipeline |
|---|---|
| `change_type` to `string_comma_decimal` | Cast error in the staging model, from that batch on |
| `rename_column` or `drop_column` | The old column is NULL in new rows, so `not_null` fails |
| `rename_column` from batch 0 | The column never exists, so the staging model fails |
| `add_column`, `change_type` to `string` | Nothing: staging selects fixed columns, and the text is the same |

The last row is not in the demo, because nothing can be expected to fail.

## Checking a run

`datachaos validate` has two levels, and they answer different questions:

| | `validate DIR` | `validate DIR --reproduce` |
|---|---|---|
| Question | Do the files on disk agree with the manifest and the ledger? | Would DataChaos write these files again? |
| Hashes, columns, schema changes | yes | yes |
| Every ledger line can be observed | yes | yes |
| Scenario invariants | no | yes, in a new run of the config |
| Needs the same DataChaos version | no | yes, or the files differ |

`validate` never reports what it cannot prove. Row IDs are not in the data by default, so ledger lines
are checked through the key. A line whose key is shared by several rows or is NULL counts as
unverifiable, with a warning and exit code 0. With `--emit-row-ids` every line is checked exactly. With
`allow_overlap`, only the last line per row and column counts, and the earlier ones are reported as
superseded.

## Adding a scenario to earlier output

```console
$ datachaos inject --input full --out more --scenario late-arriving-data --table orders --param count=5 --param delay_min=6h --param delay_max=1d
datachaos: warning: scenarios[1]: orders.customer_id is not nullable in the schema, but 10 rows now hold NULL
wrote more (run 7817a2a143c43c98)
  based on run e31a5622bdf493fe
  customers: 100 rows
  orders: 1020 rows
  order_events: 3300 rows
  scenario 0: duplicate-business-key on orders, 20 ledger lines
  scenario 1: null-critical-column on orders, 10 ledger lines
  scenario 2: invalid-enum on orders, 10 ledger lines
  scenario 3: late-arriving-data on orders, 9 ledger lines
```

`inject` does not read the data files, since row IDs are not on disk. It runs the config in the manifest
again and requires the same content, which needs the same DataChaos version. Then it adds the scenario
at the end and writes everything to a new directory. `run.based_on` in the new manifest says which run
it builds on.

## Determinism

The same DataChaos version and config give byte-identical data files, ledger and schema files, and a
manifest that differs only in `run.created_at`. CI checks this on Python 3.10 to 3.13, and on Linux,
macOS and Windows. All randomness builds on `random.Random.random()` alone, the only method whose
sequence Python guarantees across versions.

The guarantee does not cover:

- **Another DataChaos version.** A new version can change the data, and `run_id` changes with it.
  `generator_version` in the manifest and `version` per scenario say when the generator or a scenario
  has changed.
- **`run.created_at`**, which is the only value in the output that comes from the system clock.
- **Your own data.** DataChaos only works on the datasets it generates, whose schema it knows.

## Output

```text
chaos/
  chaos-manifest.json                what ran, with the hash of every file
  chaos-ledger.jsonl                 one line per change
  data/<table>/batch_0000.csv        and/or .jsonl with --format jsonl|both
  schemas/<table>/batch_0000.json    each batch has its own schema
  data/<table>.csv                   only with --combined
```

**NULL is an empty field in CSV.** The generator never writes an empty string, so this is unambiguous,
and DuckDB and dbt seeds read it the same way. A row belongs to the batch its `ingested_at` falls in,
and always has the columns of that batch. Output is written through a temp directory and renamed into
place, and an input is never overwritten.

Every format is described in
[docs/output-format.md](https://github.com/alexD1990/datachaos/blob/master/docs/output-format.md).

With `--output json`, every command except `--help` writes one JSON document with `schema_version` and
`tool_version` to stdout, and warnings to stderr.

| Exit code | Meaning |
|---|---|
| 0 | OK |
| 1 | The run completed, but a validation or an expectation failed |
| 2 | Usage, config, IO or tool error |

## Development

```bash
pip install -e ".[dev]"
ruff check && ruff format --check && mypy && pytest
pytest -m "not dbt"                             # fast
pytest -m dbt                                   # the demo against real dbt, needs .[dev,dbt]
bash .github/scripts/smoke-wheel.sh             # build, and use the wheel in a clean venv
DATACHAOS_UPDATE_GOLDEN=1 pytest tests/golden   # only when a change in output is intended
python -m datachaos._docs                       # regenerate docs/scenarios.md
```

A test runs the `datachaos` commands in this README and compares their output.

## License

MIT
