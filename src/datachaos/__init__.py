"""datachaos: controlled data quality faults, and a check that the pipeline caught them."""

from __future__ import annotations

from pathlib import Path

from datachaos import config as _config
from datachaos import engine as _engine
from datachaos._version import __version__
from datachaos.model.manifest import Manifest

__all__ = ["Manifest", "__version__", "generate", "run"]


def generate(
    out: str | Path,
    *,
    seed: int = _config.DEFAULT_SEED,
    customers: int = _config.DEFAULT_CUSTOMERS,
    orders: int = _config.DEFAULT_ORDERS,
    batches: int = _config.DEFAULT_BATCHES,
    batch_interval: str = _config.DEFAULT_BATCH_INTERVAL,
    as_of: str = _config.DEFAULT_AS_OF,
    formats: tuple[str, ...] = ("csv",),
    combined: bool = False,
    emit_row_ids: bool = False,
) -> Manifest:
    """Writes the clean `shop` dataset to `out`, which must be missing or empty.

    Raises DataChaosError for invalid settings or when the output cannot be written.
    """
    config = _config.RunConfig(
        seed=seed,
        dataset=_config.DatasetConfig(
            as_of=as_of,
            customers=customers,
            orders=orders,
            batches=batches,
            batch_interval=batch_interval,
        ),
        output=_config.OutputConfig(formats=formats, combined=combined, emit_row_ids=emit_row_ids),
    )
    return _engine.generate(config, Path(out))


def run(config: str | Path, out: str | Path, *, force: bool = False) -> Manifest:
    """Runs a config file, writing to `out`. With `force`, replaces earlier DataChaos output.

    Raises DataChaosError for an invalid config or when the output cannot be written.
    """
    return _engine.run(_config.load_config(Path(config)), Path(out), force=force)
