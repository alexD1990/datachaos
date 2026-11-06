from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable
from importlib.metadata import version

import pytest

from datachaos import __version__, cli
from datachaos.errors import DataChaosError, ValidationFailed


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
