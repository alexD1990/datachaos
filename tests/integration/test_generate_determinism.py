"""Acceptance for phase 1: two separate processes give identical files."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _generate(out: Path, *flags: str, hash_seed: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "datachaos", "generate", "--out", str(out), *flags],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONHASHSEED": hash_seed},
    )


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _without_created_at(manifest: bytes) -> dict[str, object]:
    document = json.loads(manifest)
    assert isinstance(document["run"].pop("created_at"), str)
    return document


def test_same_seed_gives_identical_files_across_hash_seeds(tmp_path: Path) -> None:
    flags = ("--seed", "1", "--batches", "3", "--format", "both", "--combined")
    _generate(tmp_path / "a", *flags, hash_seed="1")
    _generate(tmp_path / "b", *flags, hash_seed="2")
    a = _tree(tmp_path / "a")
    b = _tree(tmp_path / "b")
    assert sorted(a) == sorted(b)
    manifest_a = a.pop("chaos-manifest.json")
    manifest_b = b.pop("chaos-manifest.json")
    assert a == b
    assert _without_created_at(manifest_a) == _without_created_at(manifest_b)


def test_another_seed_gives_other_content(tmp_path: Path) -> None:
    _generate(tmp_path / "a", "--seed", "1", hash_seed="1")
    _generate(tmp_path / "b", "--seed", "2", hash_seed="1")
    a = json.loads((tmp_path / "a" / "chaos-manifest.json").read_bytes())
    b = json.loads((tmp_path / "b" / "chaos-manifest.json").read_bytes())
    assert a["content_sha256"] != b["content_sha256"]
    assert a["run"]["run_id"] != b["run"]["run_id"]
