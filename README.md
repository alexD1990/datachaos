# DataChaos

DataChaos will generate a synthetic, deterministic dataset delivered in batches, inject
controlled data quality faults, keep a ledger of exactly what changed, and check whether your
pipeline's dbt tests caught the faults.

**Status:** early development. Only `datachaos --version` works so far; none of the above is
implemented yet.

## Install

From a checkout, with Python 3.10 or newer:

```bash
pip install -e ".[dev]"
```

```console
$ datachaos --version
datachaos 0.1.0
```

## Development

```bash
ruff check && ruff format --check && mypy && pytest
```

## License

MIT
