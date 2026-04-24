"""__main__ tests around persistence wiring + import-record dispatch."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from tennis_booking import __main__ as cli


def test_resolve_store_path_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TENNIS_BOOKINGS_FILE", raising=False)
    assert cli._resolve_store_path() == cli.DEFAULT_BOOKINGS_FILE


def test_resolve_store_path_env_override(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "custom.jsonl"
    monkeypatch.setenv("TENNIS_BOOKINGS_FILE", str(target))
    assert cli._resolve_store_path() == target


def test_resolve_store_path_blank_env_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENNIS_BOOKINGS_FILE", "   ")
    assert cli._resolve_store_path() == cli.DEFAULT_BOOKINGS_FILE


def test_import_record_subcommand_runs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`python -m tennis_booking import-record ...` writes to env-configured store."""
    monkeypatch.setenv("TENNIS_BOOKINGS_FILE", str(tmp_path / "x.jsonl"))
    rc = asyncio.run(
        cli.main(
            [
                "import-record",
                "--record-id", "100",
                "--record-hash", "h100",
                "--slot-dt-local", "2026-04-26T18:00:00+05:00",
                "--court-id", "5",
                "--service-id", "11",
                "--profile", "roman",
            ]
        )
    )
    assert rc == 0
    out_file = tmp_path / "x.jsonl"
    assert out_file.exists()
    rec = json.loads(out_file.read_text(encoding="utf-8").strip())
    assert rec["record_id"] == 100
    assert rec["phase"] == "manual"


def test_import_record_subcommand_uses_explicit_store_path(tmp_path: Path) -> None:
    """--store-path overrides env."""
    target = tmp_path / "explicit.jsonl"
    rc = asyncio.run(
        cli.main(
            [
                "import-record",
                "--record-id", "200",
                "--record-hash", "h200",
                "--slot-dt-local", "2026-04-26T18:00:00+05:00",
                "--court-id", "5",
                "--service-id", "11",
                "--profile", "roman",
                "--store-path", str(target),
                "--phase", "window",
            ]
        )
    )
    assert rc == 0
    rec = json.loads(target.read_text(encoding="utf-8").strip())
    assert rec["record_id"] == 200
    assert rec["phase"] == "window"


def test_main_subcommand_dispatch_does_not_setup_logging(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Subcommand path must NOT invoke setup_logging / load_app_config / Altegio."""
    monkeypatch.setenv("TENNIS_BOOKINGS_FILE", str(tmp_path / "x.jsonl"))
    monkeypatch.setenv("TENNIS_LOG_DIR", "/this/path/does/not/exist/and/cannot/be/created/__test__")
    # If main went through _run path, setup_logging would crash on bad dir.
    rc = asyncio.run(
        cli.main(
            [
                "import-record",
                "--record-id", "1",
                "--record-hash", "h",
                "--slot-dt-local", "2026-04-26T18:00:00+05:00",
                "--court-id", "5",
                "--service-id", "11",
                "--profile", "roman",
            ]
        )
    )
    assert rc == 0


def test_parse_args_no_subcommand_default() -> None:
    ns = cli._parse_args([])
    assert ns.subcommand is None
