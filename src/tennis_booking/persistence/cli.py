"""`python -m tennis_booking import-record ...` — manual append.

Used to seed the JSONL store with bookings made outside the service (e.g.
manually through the Altegio mobile app), so the dedup check can skip
them at recompute / pre-prearm time.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from tennis_booking.common.tz import ALMATY

from .models import SCHEMA_VERSION, BookedSlot
from .store import FileBookingStore

__all__ = ["add_import_record_subparser", "run_import_record"]


def add_import_record_subparser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    sp = subparsers.add_parser(
        "import-record",
        help="Append one BookedSlot to the JSONL store (manual / one-off bookings).",
    )
    sp.add_argument("--record-id", type=int, required=True, help="Altegio record_id (>=1).")
    sp.add_argument("--record-hash", type=str, required=True, help="Altegio record_hash (non-empty).")
    sp.add_argument(
        "--slot-dt-local",
        type=str,
        required=True,
        help=(
            "Slot moment in ISO 8601 with TZ-suffix in Asia/Almaty, "
            "e.g. 2026-04-26T18:00:00+05:00"
        ),
    )
    sp.add_argument("--court-id", type=int, required=True, help="Court / staff_id (>=1).")
    sp.add_argument("--service-id", type=int, required=True, help="Service id (>=1).")
    sp.add_argument(
        "--profile",
        dest="profile_name",
        type=str,
        required=True,
        help="Profile name from profiles.yaml ([a-z0-9_-]+).",
    )
    sp.add_argument(
        "--phase",
        type=str,
        choices=("window", "poll", "manual"),
        default="manual",
        help="Booking phase tag (default: manual).",
    )
    sp.add_argument(
        "--booked-at-utc",
        type=str,
        default=None,
        help=(
            "Booking confirmation moment in ISO 8601 UTC, e.g. 2026-04-23T02:00:00+00:00. "
            "Defaults to now (UTC)."
        ),
    )
    sp.add_argument(
        "--store-path",
        type=Path,
        default=None,
        help=(
            "Override store path. If omitted, uses TENNIS_BOOKINGS_FILE env "
            "(default /app/data/bookings.jsonl)."
        ),
    )


def _parse_slot_dt_local(raw: str) -> datetime:
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as e:
        raise SystemExit(
            f"--slot-dt-local must be ISO 8601 with offset, got {raw!r}: {e}"
        ) from e
    if dt.tzinfo is None:
        raise SystemExit(
            f"--slot-dt-local must include offset (e.g. +05:00), got naive: {raw!r}"
        )
    return dt.astimezone(ALMATY)


def _parse_booked_at_utc(raw: str | None) -> datetime:
    if raw is None:
        return datetime.now(tz=UTC)
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as e:
        raise SystemExit(
            f"--booked-at-utc must be ISO 8601 with offset, got {raw!r}: {e}"
        ) from e
    if dt.tzinfo is None:
        raise SystemExit(
            f"--booked-at-utc must include offset (use +00:00), got naive: {raw!r}"
        )
    return dt.astimezone(UTC)


async def run_import_record(args: argparse.Namespace, default_store_path: Path) -> int:
    """Append one BookedSlot to the JSONL store. Async because it is invoked
    from the event-loop-running `__main__.main()`.
    """
    store_path: Path = args.store_path if args.store_path is not None else default_store_path
    slot_dt_local = _parse_slot_dt_local(args.slot_dt_local)
    booked_at_utc = _parse_booked_at_utc(args.booked_at_utc)

    try:
        slot = BookedSlot(
            schema_version=SCHEMA_VERSION,
            record_id=args.record_id,
            record_hash=args.record_hash,
            slot_dt_local=slot_dt_local,
            court_id=args.court_id,
            service_id=args.service_id,
            profile_name=args.profile_name,
            phase=args.phase,
            booked_at_utc=booked_at_utc,
        )
    except ValueError as e:
        raise SystemExit(f"invalid BookedSlot: {e}") from e

    try:
        store = FileBookingStore(store_path)
    except ValueError as e:
        raise SystemExit(f"cannot open store at {store_path}: {e}") from e

    await store.append(slot)
    print(
        f"appended record_id={slot.record_id} court_id={slot.court_id} "
        f"slot_dt_local={slot.slot_dt_local.isoformat()} → {store_path}"
    )
    return 0
