from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tennis_booking.persistence.cli import (
    add_import_record_subparser,
    run_import_record,
)


def _run(args: argparse.Namespace, default_store_path: Path) -> int:
    return asyncio.run(run_import_record(args, default_store_path))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="subcommand")
    add_import_record_subparser(sub)
    return parser


def _args(tmp_path: Path, **overrides: str) -> argparse.Namespace:
    parser = _build_parser()
    base = [
        "import-record",
        "--record-id", "5555",
        "--record-hash", "manualhash",
        "--slot-dt-local", "2026-04-26T18:00:00+05:00",
        "--court-id", "7",
        "--service-id", "99",
        "--profile", "roman",
        "--store-path", str(tmp_path / "bookings.jsonl"),
    ]
    cli = []
    for k, v in overrides.items():
        cli.extend([f"--{k.replace('_', '-')}", v])
    return parser.parse_args(base + cli)


def test_default_phase_is_manual(tmp_path: Path) -> None:
    args = _args(tmp_path)
    assert args.phase == "manual"


def test_appends_record(tmp_path: Path) -> None:
    args = _args(tmp_path)
    rc = _run(args, default_store_path=Path("/unused"))
    assert rc == 0
    out = tmp_path / "bookings.jsonl"
    assert out.exists()
    data = json.loads(out.read_text(encoding="utf-8").strip())
    assert data["record_id"] == 5555
    assert data["phase"] == "manual"
    assert data["court_id"] == 7
    assert data["service_id"] == 99
    assert data["profile_name"] == "roman"
    assert data["slot_dt_local"] == "2026-04-26T18:00:00+05:00"


def test_default_store_path_used_when_not_overridden(tmp_path: Path) -> None:
    parser = _build_parser()
    args = parser.parse_args(
        [
            "import-record",
            "--record-id", "1",
            "--record-hash", "h",
            "--slot-dt-local", "2026-04-26T18:00:00+05:00",
            "--court-id", "7",
            "--service-id", "99",
            "--profile", "roman",
        ]
    )
    default_path = tmp_path / "default.jsonl"
    rc = _run(args, default_store_path=default_path)
    assert rc == 0
    assert default_path.exists()


def test_invalid_slot_dt_naive_exits(tmp_path: Path) -> None:
    args = _args(tmp_path, slot_dt_local="2026-04-26T18:00:00")
    with pytest.raises(SystemExit, match="offset"):
        _run(args, default_store_path=tmp_path / "bookings.jsonl")


def test_invalid_slot_dt_garbage_exits(tmp_path: Path) -> None:
    args = _args(tmp_path, slot_dt_local="not-a-date")
    with pytest.raises(SystemExit, match="ISO 8601"):
        _run(args, default_store_path=tmp_path / "bookings.jsonl")


def test_invalid_booked_at_naive_exits(tmp_path: Path) -> None:
    args = _args(tmp_path, booked_at_utc="2026-04-23T02:00:00")
    with pytest.raises(SystemExit, match="offset"):
        _run(args, default_store_path=tmp_path / "bookings.jsonl")


def test_phase_window_accepted(tmp_path: Path) -> None:
    args = _args(tmp_path, phase="window")
    rc = _run(args, default_store_path=Path("/unused"))
    assert rc == 0
    data = json.loads((tmp_path / "bookings.jsonl").read_text(encoding="utf-8").strip())
    assert data["phase"] == "window"


def test_invalid_record_id_below_one_exits(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.record_id = 0
    with pytest.raises(SystemExit, match="invalid BookedSlot"):
        _run(args, default_store_path=tmp_path / "bookings.jsonl")


def test_missing_parent_dir_exits(tmp_path: Path) -> None:
    args = _args(tmp_path, store_path=str(tmp_path / "no-such" / "x.jsonl"))
    with pytest.raises(SystemExit, match="cannot open store"):
        _run(args, default_store_path=tmp_path / "bookings.jsonl")


def test_booked_at_utc_explicit(tmp_path: Path) -> None:
    args = _args(tmp_path, booked_at_utc="2026-04-23T02:00:00+00:00")
    rc = _run(args, default_store_path=Path("/unused"))
    assert rc == 0
    data = json.loads((tmp_path / "bookings.jsonl").read_text(encoding="utf-8").strip())
    assert data["booked_at_utc"] == "2026-04-23T02:00:00+00:00"


def test_booked_at_utc_default_is_now(tmp_path: Path) -> None:
    args = _args(tmp_path)
    before = datetime.now(tz=UTC)
    rc = _run(args, default_store_path=Path("/unused"))
    after = datetime.now(tz=UTC)
    assert rc == 0
    data = json.loads((tmp_path / "bookings.jsonl").read_text(encoding="utf-8").strip())
    booked = datetime.fromisoformat(data["booked_at_utc"])
    assert before <= booked <= after
