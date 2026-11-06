"""Safe writing: everything goes to a temp directory that is renamed into place at the end."""

from __future__ import annotations

import hashlib
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from datachaos.errors import DataChaosError
from datachaos.io.reader import read_json
from datachaos.model.manifest import MANIFEST_FILE


@contextmanager
def atomic_output(out: Path, run_id: str, *, force: bool = False) -> Iterator[Path]:
    """Yields a temp directory, and publishes it as `out` when the block succeeds.

    `out` must be missing or empty. With `force` it may also be earlier DataChaos output, which
    is only removed once the new output is complete. Missing parents are created. A temp
    directory left by an earlier run is an error, since only what this run created may be
    deleted.
    """
    target = out.resolve()
    replacing = _check_target(target, force=force)
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
    previous = target.with_name(f"{target.name}.old-{run_id}")
    try:
        yield temp
        if replacing:
            _replace(temp, target, previous)
        else:
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


def _replace(temp: Path, target: Path, previous: Path) -> None:
    if previous.exists():
        raise DataChaosError(
            f"directory {previous} already exists; remove it if no other run is using it"
        )
    os.rename(target, previous)
    try:
        os.replace(temp, target)
    except OSError:
        os.rename(previous, target)
        raise
    shutil.rmtree(previous)


def _check_target(target: Path, *, force: bool) -> bool:
    """Returns whether `target` holds earlier output to replace."""
    if not target.name:
        raise DataChaosError(f"cannot write output to {target}")
    if not target.exists():
        return False
    if not target.is_dir():
        raise DataChaosError(f"output path {target} exists and is not a directory")
    if not any(target.iterdir()):
        return False
    if not force:
        raise DataChaosError(f"output directory {target} is not empty")
    _check_replaceable(target)
    return True


def _check_replaceable(target: Path) -> None:
    """--force only removes what an earlier run wrote, as listed in its manifest."""
    if not (target / MANIFEST_FILE).is_file():
        raise DataChaosError(f"--force: {target} has no {MANIFEST_FILE}, so it is not left alone")
    try:
        allowed = {entry["path"] for entry in read_json(target / MANIFEST_FILE)["files"]}
    except (KeyError, TypeError) as exc:
        raise DataChaosError(f"--force: {target / MANIFEST_FILE} is not a manifest") from exc
    allowed.add(MANIFEST_FILE)
    for path in sorted(target.rglob("*")):
        relative = path.relative_to(target).as_posix()
        is_plain_directory = path.is_dir() and not path.is_symlink()
        if not is_plain_directory and relative not in allowed:
            raise DataChaosError(
                f"--force: {relative} in {target} is not listed in its manifest; "
                "move it or choose another directory"
            )
