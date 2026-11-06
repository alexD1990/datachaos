from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from datachaos.errors import DataChaosError
from datachaos.io.output_dir import atomic_output, sha256_file


def _write(root: Path) -> None:
    (root / "file.txt").write_bytes(b"content")


def test_publishes_into_a_missing_directory_and_leaves_no_temp(tmp_path: Path) -> None:
    out = tmp_path / "out"
    with atomic_output(out, "abc") as root:
        assert root.name == "out.tmp-abc"
        assert not out.exists()
        _write(root)
    assert (out / "file.txt").read_bytes() == b"content"
    assert [path.name for path in tmp_path.iterdir()] == ["out"]


def test_creates_missing_parents(tmp_path: Path) -> None:
    out = tmp_path / "a" / "b" / "out"
    with atomic_output(out, "abc") as root:
        _write(root)
    assert (out / "file.txt").exists()


def test_publishes_into_an_empty_directory(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    with atomic_output(out, "abc") as root:
        _write(root)
    assert (out / "file.txt").exists()


def test_non_empty_directory_is_refused_and_untouched(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    (out / "keep.txt").write_bytes(b"keep")
    with pytest.raises(DataChaosError, match="is not empty"), atomic_output(out, "abc"):
        pass
    assert (out / "keep.txt").read_bytes() == b"keep"
    assert [path.name for path in tmp_path.iterdir()] == ["out"]


def test_a_file_at_the_output_path_is_refused(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.write_bytes(b"keep")
    with pytest.raises(DataChaosError, match="not a directory"), atomic_output(out, "abc"):
        pass
    assert out.read_bytes() == b"keep"


def test_a_file_as_parent_is_refused(tmp_path: Path) -> None:
    parent = tmp_path / "parent"
    parent.write_bytes(b"keep")
    with pytest.raises(DataChaosError, match="cannot create"), atomic_output(parent / "out", "abc"):
        pass
    assert parent.read_bytes() == b"keep"


def test_an_old_temp_directory_is_an_error_and_is_not_deleted(tmp_path: Path) -> None:
    temp = tmp_path / "out.tmp-abc"
    temp.mkdir()
    (temp / "old.txt").write_bytes(b"old")
    with (
        pytest.raises(DataChaosError, match="already exists"),
        atomic_output(tmp_path / "out", "abc"),
    ):
        pass
    assert (temp / "old.txt").read_bytes() == b"old"
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize(
    ("raised", "expected"),
    [(RuntimeError("boom"), RuntimeError), (OSError("disk full"), DataChaosError)],
)
def test_failure_removes_the_temp_directory_and_publishes_nothing(
    tmp_path: Path, raised: Exception, expected: type[Exception]
) -> None:
    out = tmp_path / "out"
    with pytest.raises(expected), atomic_output(out, "abc") as root:
        _write(root)
        raise raised
    assert list(tmp_path.iterdir()) == []


def test_sha256_file_hashes_bytes_including_invalid_utf8(tmp_path: Path) -> None:
    path = tmp_path / "f"
    path.write_bytes(b"\xff\r\n")
    assert sha256_file(path) == hashlib.sha256(b"\xff\r\n").hexdigest()
