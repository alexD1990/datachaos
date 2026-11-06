"""Adds a scenario to earlier output, by running its config again with the scenario at the end.

Row IDs are not on disk, so the data files are not read. Regenerating is only valid if it gives
what the input holds, so that is checked first.
"""

from __future__ import annotations

import tempfile
from dataclasses import replace
from pathlib import Path

from datachaos import engine
from datachaos._version import __version__
from datachaos.config import ScenarioConfig, parse_config
from datachaos.errors import DataChaosError
from datachaos.model.manifest import Manifest
from datachaos.validate import check_hashes, load_manifest


def inject(input_dir: Path, out: Path, scenario: ScenarioConfig) -> Manifest:
    source = input_dir.resolve()
    target = out.resolve()
    if target == source or source in target.parents:
        raise DataChaosError(f"--out {out} must be outside --input {input_dir}")
    manifest = load_manifest(input_dir)
    changed = check_hashes(input_dir, manifest)
    if changed:
        raise DataChaosError(
            f"{input_dir} does not match its manifest ({changed[0].path}: {changed[0].message}), "
            "so it cannot be regenerated; run datachaos validate"
        )
    config = parse_config(manifest["run"]["config"])
    with tempfile.TemporaryDirectory(prefix="datachaos-inject-") as temp:
        regenerated = engine.run(config, Path(temp) / "out")
    if regenerated.content_sha256 != manifest["content_sha256"]:
        raise DataChaosError(
            f"regenerating {input_dir} from its manifest gives other content, so the scenario "
            f"cannot be added to it. It was written by DataChaos {manifest.get('tool_version')}, "
            f"and this is {__version__}"
        )
    return engine.run(
        replace(config, scenarios=(*config.scenarios, scenario)),
        out,
        based_on={
            "run_id": manifest["run"]["run_id"],
            "content_sha256": manifest["content_sha256"],
        },
    )
