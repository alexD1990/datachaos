"""Safe writing: everything goes to a temp directory that is renamed into place at the end."""

from __future__ import annotations

import hashlib
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from datachaos.errors import DataChaosError


@contextmanager
def atomic_output(out: Path, run_id: str) -> Iterator[Path]:
    """Yields a temp directory, and publishes it as `out` when the block succeeds.

    `out` must be missing or empty. Missing parents are created. A temp directory left by an
    earlier run is an error, since only what this run created may be deleted.
    """
    target = out.resolve()
    _check_target(target)
    temp = target.with_name(f"{target.name}.tmp-{run_id}")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DataChaosError(f"cannot create {target.parent}: {exc}") from exc
    try:
        temp.mkdir()
    except FileExistsError as exc:
        raise DataChaosError(
            f"temp directory {temp} already exists; remove it if no other run is using it"
        ) from exc
    except OSError as exc:
        raise DataChaosError(f"cannot create {temp}: {exc}") from exc
    try:
        yield temp
        if target.exists():
            target.rmdir()
        os.replace(temp, target)
    except OSError as exc:
        shutil.rmtree(temp, ignore_errors=True)
        raise DataChaosError(f"cannot write {target}: {exc}") from exc
    except BaseException:
        shutil.rmtree(temp, ignore_errors=True)
        raise


def sha256_file(path: Path) -> str:
    """Hashes the bytes on disk, so binary mode: a corrupted file must hash, not fail to decode."""
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _check_target(target: Path) -> None:
    if not target.name:
        raise DataChaosError(f"cannot write output to {target}")
    if not target.exists():
        return
    if not target.is_dir():
        raise DataChaosError(f"output path {target} exists and is not a directory")
    if any(target.iterdir()):
        raise DataChaosError(f"output directory {target} is not empty")
