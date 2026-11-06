# Output format

What DataChaos writes, in the words of schema version 1.0. Minor versions only add fields, so ignore
fields you do not know.

## Files

```text
<out>/
  chaos-manifest.json
  chaos-ledger.jsonl
  data/<table>/batch_0000.csv        and/or .jsonl
  schemas/<table>/batch_0000.json
  data/<table>.csv                   only with --combined, and when all batches have the same schema
```

Every batch has its files. A batch without rows is a CSV with only the header and an empty JSONL file.
`chaos-ledger.jsonl` always exists, and is empty when no scenario has run.

## CSV

- UTF-8 without BOM, `\n` as line ending on every operating system, with a header.
- Fields are quoted only when they must be, so `12,50` is written as `"12,50"`.
- **NULL is an empty field.** The generator never writes an empty string, so this is unambiguous.
- Decimals have a fixed number of decimals, booleans are `true` or `false`, timestamps are
  `YYYY-MM-DDTHH:MM:SSZ`, and dates are `YYYY-MM-DD`.
- After `schema-drift`, values follow the schema of their batch.

## JSONL

One object per line, with keys in schema order and no spaces. NULL is `null`, and decimals and times are
strings.

## Schema files

One per table and batch:

```json
{
  "schema_version": "1.0",
  "tool_version": "0.1.0",
  "table": "orders",
  "kind": "entity",
  "batch": 0,
  "columns": [
    {"name": "order_id", "type": "string", "nullable": false, "role": "key"}
  ]
}
```

- `type` is `string`, `integer`, `decimal`, `boolean`, `date` or `timestamp`.
- `role` is `key`, `foreign_key`, `event_time`, `ingestion_time`, `updated_at`, `enum` or `attribute`.
- `references`, `allowed_values` and `scale` are there only when they are set.
- With `--emit-row-ids`, `_chaos_rid` is the last column of both the data and the schema.

## Manifest

`chaos-manifest.json` is described by
[schema/chaos-manifest-1.0.schema.json](https://github.com/alexD1990/datachaos/blob/master/schema/chaos-manifest-1.0.schema.json).

| Field | Content |
|---|---|
| `schema_version`, `tool_version` | The versions of the format and of DataChaos |
| `run.run_id` | The first 16 hex digits of the SHA-256 of the resolved config and the version |
| `run.created_at` | When it ran, the only value that differs between two runs |
| `run.seed`, `run.config` | The seed and the fully resolved config, which can run again |
| `run.based_on` | After `inject`: `run_id` and `content_sha256` of the run it builds on |
| `dataset` | Name, `generator_version`, `as_of`, batches and rows per table before and after scenarios |
| `scenarios` | Per scenario: index, name, version, table, seed, resolved params, stats and number of ledger lines |
| `files` | Path, SHA-256 and, for data and the ledger, rows. Sorted on path, with `/` on every system |
| `content_sha256` | SHA-256 over the line `<sha256>  <path>\n` per data file and the ledger, sorted on path |
| `warnings` | For example NULL in a column that the schema says is not nullable |

`files` lists the ledger, the data files and the schema files, but not the manifest itself.
`content_sha256` does not cover the schema files.

## Ledger

`chaos-ledger.jsonl` has one line per change, sorted on scenario, table, row and column:

```json
{"scenario_index":1,"scenario":"null-critical-column","table":"orders","action":"set_value","rid":"o000052","key":{"order_id":"O000052"},"batch":0,"column":"customer_id","before":"C000031","after":null}
```

| `action` | Meaning | `column`, `before`, `after` |
|---|---|---|
| `insert_row` | A new row. `source_rid` says which it comes from | Set if a value differs from the source |
| `set_value` | A value changed | The column, and its old and new value |
| `move_batch` | The row moved to another batch, after a `set_value` on `ingested_at` | `before` and `after` are batches |
| `delete_row` | The row is gone | Not set |
| `orphan_row` | The row lost its parent without changing itself | The foreign key, with `before` equal to `after` |
| `schema_change` | One line per `schema-drift`: `rid` is empty, `key` is `{}`, and `batch` is `from_batch` | The column, and its old and new name or type |

- `rid` is an internal row ID, which is not in the data without `--emit-row-ids`. New rows from scenario
  `i` are `s{i:04d}-{n:07d}`.
- `key` is the value when the line was written, so a key set to NULL still shows the old one.
- `group`, after `key`, is the group a scenario worked on, such as `{"order_id":"O000031"}`. Later
  scenarios stay away from all of its rows.

## Landing directory

`datachaos land` writes data to `<landing>/<table>/batch_0000.csv` and schemas to
`<landing>/schemas/<table>/batch_0000.json`, and neither the manifest nor the ledger.

## `--output json`

Every document starts with `schema_version` and `tool_version`.

| Command | Fields |
|---|---|
| `generate`, `run`, `inject` | `status`, `directory`, `run_id`, `based_on` after `inject`, `content_sha256`, `tables`, `scenarios`, `warnings` |
| `validate` | `status`, `directory`, `reproduce`, `ledger` (`total`, `superseded`, `verified`, `unverifiable`), `unverifiable`, `problems` (`check`, `path`, `message`), `warnings` |
| `report` | `directory`, `run_id`, `seed`, `created_at`, `based_on` after `inject`, `as_of`, `batches`, `tables`, `scenarios`, `ledger_lines`, `warnings` |
| `land` | `status`, `directory`, `upto_batch`, `copied`, `skipped` |
| `check-dbt` | `status`, `directory`, `run_results`, `baseline`, `nodes`, `expectations` (`scenario`, `failing_test`, `detected`, `nodes`), `unexpected` |
| `list-scenarios` | `scenarios` |
| `describe` | `scenario` |
