"""Copies batches to a landing directory, as they would arrive. Never deletes or overwrites."""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from datachaos.errors import DataChaosError
from datachaos.io.output_dir import sha256_file
from datachaos.validate import is_safe_path, load_manifest

_BATCH_FILE = re.compile(r"(data|schemas)/([^/]+)/(batch_([0-9]+)\.[a-z]+)")


@dataclass(frozen=True)
class LandResult:
    directory: str
    upto_batch: int
    copied: int
    skipped: int

    def to_dict(self) -> dict[str, object]:
        return {
            "status": "ok",
            "directory": self.directory,
            "upto_batch": self.upto_batch,
            "copied": self.copied,
            "skipped": self.skipped,
        }


def land(directory: Path, upto_batch: int, to: Path) -> LandResult:
    """Everything is checked before the first file is copied, so a conflict copies nothing."""
    source = directory.resolve()
    target = to.resolve()
    if target == source or source in target.parents or target in source.parents:
        raise DataChaosError(f"--to {to} and {directory} must not overlap")
    manifest = load_manifest(directory)
    batches = manifest["dataset"]["batches"]
    if not 0 <= upto_batch < batches:
        raise DataChaosError(f"--upto-batch must be from 0 to {batches - 1}, got {upto_batch}")

    to_copy = []
    skipped = 0
    for path, sha256, relative in _files_to_land(manifest, upto_batch):
        if not is_safe_path(path) or not (directory / path).is_file():
            raise DataChaosError(f"{directory}: {path} is missing; run datachaos validate")
        if sha256_file(directory / path) != sha256:
            raise DataChaosError(
                f"{directory}: {path} does not match its hash; run datachaos validate"
            )
        destination = target / relative
        if destination.exists():
            if not destination.is_file() or sha256_file(destination) != sha256:
                raise DataChaosError(
                    f"{destination} exists with other content; land never overwrites"
                )
            skipped += 1
            continue
        temp = destination.with_name(f".{destination.name}.tmp")
        if temp.exists():
            raise DataChaosError(f"{temp} already exists; remove it if no other run is using it")
        to_copy.append((directory / path, destination, temp))

    try:
        for path_from, destination, temp in to_copy:
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copyfile(path_from, temp)
                os.rename(temp, destination)
            finally:
                temp.unlink(missing_ok=True)
    except OSError as exc:
        raise DataChaosError(f"cannot write to {to}: {exc}") from exc
    return LandResult(str(to), upto_batch, len(to_copy), skipped)


def _files_to_land(manifest: Mapping[str, Any], upto_batch: int) -> list[tuple[str, str, str]]:
    tables = manifest["dataset"]["tables"]
    files = []
    for entry in manifest["files"]:
        match = _BATCH_FILE.fullmatch(entry["path"])
        if match is None or match.group(2) not in tables or int(match.group(4)) > upto_batch:
            continue
        kind, table, name = match.group(1), match.group(2), match.group(3)
        relative = f"{table}/{name}" if kind == "data" else f"schemas/{table}/{name}"
        files.append((entry["path"], entry["sha256"], relative))
    return files
