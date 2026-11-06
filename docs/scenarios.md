# Scenarios

Generated from the scenario specs by `python -m datachaos._docs`. Do not edit by hand.

## `duplicate-business-key`

Adds a second row with the same business key.

- **Version:** 1
- **Detects:** Broken grain, and staging models that do not deduplicate.
- **Typical dbt test:** `unique`
- **Applies to:** entity tables

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `rate` | float or int | no | `none` | Share of the candidate rows to select. Give exactly one of rate and count. |
| `count` | int | no | `none` | Number of candidate rows to select. Give exactly one of rate and count. |
| `allow_overlap` | bool | no | `false` | Also select rows whose key an earlier scenario has touched. |
| `key_columns` | list | no | `none` | Columns that identify a row. Defaults to the columns with role KEY. |
| `mutation_mode` | str | no | `exact_copy` | exact_copy, or mutate_non_key to change one other column on the copy. |
| `same_batch` | bool | no | `true` | Keep the copy in the batch of the original, or else in a later one. |

## `invalid-enum`

Replaces a category with a value outside the allowed values.

- **Version:** 1
- **Detects:** Unknown categories that fall out of CASE expressions and mappings.
- **Typical dbt test:** `accepted_values`
- **Applies to:** entity, event_log tables

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `rate` | float or int | no | `none` | Share of the candidate rows to select. Give exactly one of rate and count. |
| `count` | int | no | `none` | Number of candidate rows to select. Give exactly one of rate and count. |
| `allow_overlap` | bool | no | `false` | Also select rows whose key an earlier scenario has touched. |
| `column` | str | yes |  | A column with role ENUM. |
| `values` | list | no | `['UNKNOWN']` | The values to use with variant literal. Not allowed with other variants. |
| `variant` | str | no | `literal` | literal uses values. case_change turns paid into PAID, and NO into no. whitespace turns paid into 'paid '. |

## `late-arriving-data`

Delays ingested_at, so that rows land in a later batch than their event time.

- **Version:** 1
- **Detects:** Incremental models with a watermark on event time, which never pick up late rows.
- **Typical dbt test:** `singular test comparing the incremental model with a full refresh`
- **Applies to:** entity, event_log tables

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `rate` | float or int | no | `none` | Share of the candidate rows to select. Give exactly one of rate and count. |
| `count` | int | no | `none` | Number of candidate rows to select. Give exactly one of rate and count. |
| `allow_overlap` | bool | no | `false` | Also select rows whose key an earlier scenario has touched. |
| `delay_min` | str | yes |  | The shortest delay, such as 6h. |
| `delay_max` | str | yes |  | The longest delay, such as 2d. Candidates are the rows that stay before as_of with this delay, and rate is a share of those. |

## `null-critical-column`

Sets a column to NULL, whether or not the schema allows it.

- **Version:** 1
- **Detects:** Missing not_null tests, and joins that lose rows on NULL.
- **Typical dbt test:** `not_null`
- **Applies to:** entity, event_log tables

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `rate` | float or int | no | `none` | Share of the candidate rows to select. Give exactly one of rate and count. |
| `count` | int | no | `none` | Number of candidate rows to select. Give exactly one of rate and count. |
| `allow_overlap` | bool | no | `false` | Also select rows whose key an earlier scenario has touched. |
| `column` | str | yes |  | The column to set to NULL. |
| `allow_key` | bool | no | `false` | Allow a column with role KEY. |

## `orphaned-foreign-key`

Makes foreign keys point at a parent that does not exist.

- **Version:** 1
- **Detects:** Inner joins that lose rows, and missing referential integrity.
- **Typical dbt test:** `relationships`
- **Applies to:** entity, event_log tables

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `rate` | float or int | no | `none` | Share of the candidate rows to select. Give exactly one of rate and count. |
| `count` | int | no | `none` | Number of candidate rows to select. Give exactly one of rate and count. |
| `allow_overlap` | bool | no | `false` | Also select rows whose key an earlier scenario has touched. |
| `column` | str | no | `none` | The foreign key column. Defaults to the first one in the table. |
| `mode` | str | no | `nonexistent` | nonexistent points selected rows at an ID that does not exist. deleted_parent deletes selected parents, so all their children are orphaned. rate and count then select parents, among those with children. |

## `out-of-order-events`

Holds back the next to last event of a group, so that it lands after the last.

- **Version:** 1
- **Detects:** Latest-event-wins logic that sorts on ingested_at instead of event time, or that assumes events arrive in order.
- **Typical dbt test:** `singular test that the current status comes from the latest event time`
- **Applies to:** event_log tables

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `rate` | float or int | no | `none` | Share of the candidate rows to select. Give exactly one of rate and count. |
| `count` | int | no | `none` | Number of candidate rows to select. Give exactly one of rate and count. |
| `allow_overlap` | bool | no | `false` | Also select rows whose key an earlier scenario has touched. |
| `group_by` | str | no | `none` | The column that defines a group. Defaults to the foreign key. rate and count select groups, among those with two events in order and room before as_of. |
