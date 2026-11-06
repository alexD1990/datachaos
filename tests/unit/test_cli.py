from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path

import pytest

from datachaos import __version__, cli
from datachaos.errors import DataChaosError, ValidationFailed
from datachaos.io.reader import read_json


def _raiser(exc: Exception) -> Callable[[object], int]:
    def dispatch(args: object) -> int:
        raise exc

    return dispatch


def test_version_flag_prints_to_stdout_and_exits_0(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["--version"])
    assert info.value.code == 0
    captured = capsys.readouterr()
    assert captured.out == f"datachaos {__version__}\n"
    assert captured.err == ""


def test_installed_metadata_matches_package_version() -> None:
    assert version("datachaos") == __version__


def test_python_dash_m_runs_the_cli() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "datachaos", "--version"], capture_output=True, text=True
    )
    assert result.returncode == 0
    assert result.stdout == f"datachaos {__version__}\n"


def test_no_command_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main([]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "datachaos: error: no command given (see --help)\n"


def test_unknown_argument_gives_one_line_and_exit_2(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["--nope"])
    assert info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "datachaos: error: unrecognized arguments: --nope\n"


@pytest.mark.parametrize(
    ("exc", "code", "message"),
    [
        (DataChaosError("bad config"), 2, "bad config"),
        (ValidationFailed("checksum mismatch"), 1, "checksum mismatch"),
        (RuntimeError("boom"), 2, "internal error: boom"),
    ],
)
def test_errors_map_to_exit_code_without_stack_trace(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    exc: Exception,
    code: int,
    message: str,
) -> None:
    monkeypatch.setattr(cli, "_dispatch", _raiser(exc))
    assert cli.main([]) == code
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == f"datachaos: error: {message}\n"


def test_verbose_adds_stack_trace_and_keeps_the_error_line_last(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "_dispatch", _raiser(DataChaosError("bad config")))
    assert cli.main(["--verbose"]) == 2
    err = capsys.readouterr().err
    assert "Traceback (most recent call last)" in err
    assert err.endswith("datachaos: error: bad config\n")


def test_generate_writes_the_output_and_reports_on_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "parent" / "out"
    code = cli.main(
        ["generate", "--out", str(out), "--seed", "1", "--customers", "2", "--orders", "3"]
    )
    assert code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert f"wrote {out} (run " in captured.out
    assert "  orders: 3 rows\n" in captured.out
    assert (out / "chaos-manifest.json").exists()
    assert (out / "data" / "orders" / "batch_0000.csv").exists()
    assert not (out / "data" / "orders" / "batch_0000.jsonl").exists()


@pytest.mark.parametrize(
    ("flags", "present", "absent"),
    [
        (["--format", "jsonl"], ["batch_0000.jsonl"], ["batch_0000.csv"]),
        (["--format", "both"], ["batch_0000.csv", "batch_0000.jsonl"], []),
    ],
)
def test_generate_format(
    tmp_path: Path, flags: list[str], present: list[str], absent: list[str]
) -> None:
    out = tmp_path / "out"
    assert cli.main(["generate", "--out", str(out), "--orders", "5", *flags]) == 0
    for name in present:
        assert (out / "data" / "orders" / name).exists()
    for name in absent:
        assert not (out / "data" / "orders" / name).exists()


def test_generate_passes_every_setting_on(tmp_path: Path) -> None:
    out = tmp_path / "out"
    code = cli.main(
        [
            "generate",
            "--out",
            str(out),
            "--seed",
            "7",
            "--customers",
            "2",
            "--orders",
            "3",
            "--batches",
            "2",
            "--batch-interval",
            "12h",
            "--as-of",
            "2026-03-01T00:00:00Z",
            "--combined",
            "--emit-row-ids",
        ]
    )
    assert code == 0
    config = read_json(out / "chaos-manifest.json")["run"]["config"]
    assert config["seed"] == 7
    assert config["dataset"] == {
        "name": "shop",
        "as_of": "2026-03-01T00:00:00Z",
        "customers": 2,
        "orders": 3,
        "batches": 2,
        "batch_interval": "12h",
    }
    assert config["output"] == {"formats": ["csv"], "combined": True, "emit_row_ids": True}


@pytest.mark.parametrize(
    ("flags", "message"),
    [
        (["--orders", "0"], "dataset.orders: must be an integer from 1 to 899999, got 0"),
        (["--batch-interval", "2days"], "dataset.batch_interval: '2days' is not a duration"),
        (["--as-of", "yesterday"], "dataset.as_of: 'yesterday' is not a timestamp"),
    ],
)
def test_generate_with_invalid_settings_gives_exit_2_and_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], flags: list[str], message: str
) -> None:
    assert cli.main(["generate", "--out", str(tmp_path / "out"), *flags]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith(f"datachaos: error: {message}")
    assert captured.err.count("\n") == 1
    assert list(tmp_path.iterdir()) == []


def test_generate_requires_out(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["generate"])
    assert info.value.code == 2
    assert capsys.readouterr().err == (
        "datachaos: error: the following arguments are required: --out\n"
    )


def test_generate_into_non_empty_directory_gives_exit_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "keep.txt").write_bytes(b"keep")
    assert cli.main(["generate", "--out", str(tmp_path)]) == 2
    assert "is not empty" in capsys.readouterr().err
    assert (tmp_path / "keep.txt").read_bytes() == b"keep"


def test_generate_accepts_verbose_after_the_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "keep.txt").write_bytes(b"keep")
    assert cli.main(["generate", "--out", str(tmp_path), "--verbose"]) == 2
    err = capsys.readouterr().err
    assert "Traceback (most recent call last)" in err
    assert "is not empty" in err
