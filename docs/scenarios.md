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
