from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tennis_booking import __main__ as cli


@pytest.fixture(autouse=True)
def _reset_logging() -> Iterator[None]:
    """Don't let test-installed handlers leak into other tests."""
    yield
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:  # noqa: BLE001
            pass


def test_import_does_not_crash() -> None:
    # If __main__ import has a side-effect that fails, we notice here, not in CI alarm.
    assert hasattr(cli, "main")
    assert hasattr(cli, "entrypoint")


def test_parse_args_defaults() -> None:
    ns = cli._parse_args([])
    assert ns.config_dir == cli.DEFAULT_CONFIG_DIR
    assert ns.log_level == cli.DEFAULT_LOG_LEVEL
    assert ns.dry_run is False


def test_parse_args_overrides(tmp_path: Path) -> None:
    ns = cli._parse_args(
        ["--config-dir", str(tmp_path), "--log-level", "DEBUG", "--dry-run"]
    )
    assert ns.config_dir == tmp_path
    assert ns.log_level == "DEBUG"
    assert ns.dry_run is True


def test_parse_args_invalid_flag_exits() -> None:
    with pytest.raises(SystemExit) as exc_info:
        cli._parse_args(["--not-a-flag"])
    assert exc_info.value.code == 2  # argparse convention


def test_resolve_log_dir_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("TENNIS_LOG_DIR", str(tmp_path / "logs"))
    assert cli._resolve_log_dir() == tmp_path / "logs"


def test_resolve_log_dir_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TENNIS_LOG_DIR", raising=False)
    assert cli._resolve_log_dir() == cli.DEFAULT_LOG_DIR


def test_resolve_log_dir_blank_env_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENNIS_LOG_DIR", "   ")
    assert cli._resolve_log_dir() == cli.DEFAULT_LOG_DIR


def test_main_without_bearer_token_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing ALTEGIO_BEARER_TOKEN → non-zero exit, user-friendly stderr hint."""
    monkeypatch.delenv("ALTEGIO_BEARER_TOKEN", raising=False)
    monkeypatch.setenv("TENNIS_LOG_DIR", str(tmp_path / "logs"))

    config_dir = tmp_path / "cfg"
    config_dir.mkdir()

    code = asyncio.run(
        cli.main(["--config-dir", str(config_dir), "--log-level", "INFO"])
    )
    assert code == cli.EXIT_ERROR

    err = capsys.readouterr().err
    assert "ALTEGIO_BEARER_TOKEN" in err


def test_main_with_missing_config_dir_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("ALTEGIO_BEARER_TOKEN", "fake-token-for-test")
    monkeypatch.setenv("TENNIS_LOG_DIR", str(tmp_path / "logs"))

    # Config dir that does not exist
    nowhere = tmp_path / "nope"

    code = asyncio.run(
        cli.main(["--config-dir", str(nowhere), "--log-level", "INFO"])
    )
    assert code == cli.EXIT_ERROR
    err = capsys.readouterr().err
    assert "config" in err.lower()


def test_main_with_bad_log_level_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("TENNIS_LOG_DIR", str(tmp_path / "logs"))
    code = asyncio.run(
        cli.main(["--config-dir", str(tmp_path), "--log-level", "BOGUS"])
    )
    assert code == cli.EXIT_ERROR
    err = capsys.readouterr().err
    assert "logging" in err.lower()


def test_install_signal_handlers_on_windows_is_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On Windows add_signal_handler raises NotImplementedError; must not crash."""
    from tennis_booking.scheduler.loop import SchedulerLoop

    class _FakeLoop:
        def add_signal_handler(self, sig: int, callback: Any, *args: Any) -> None:
            raise NotImplementedError("windows")

    logger = logging.getLogger("test_signals")

    class _FakeScheduler:
        pass

    # We only care that the helper tolerates NotImplementedError from every add_signal_handler.
    cli._install_signal_handlers(
        _FakeLoop(),  # type: ignore[arg-type]
        _FakeScheduler(),  # type: ignore[arg-type]
        logger,
    )
    # No exception = pass. SchedulerLoop import check — the class is reachable.
    assert SchedulerLoop is not None
