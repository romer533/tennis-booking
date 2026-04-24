from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from tennis_booking.common.tz import ALMATY
from tennis_booking.persistence.models import SCHEMA_VERSION, BookedSlot
from tennis_booking.persistence.store import FileBookingStore

SLOT_LOCAL = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
BOOKED_AT = datetime(2026, 4, 23, 2, 0, tzinfo=UTC)


def _slot(**overrides: Any) -> BookedSlot:
    base: dict[str, Any] = dict(
        schema_version=SCHEMA_VERSION,
        record_id=1,
        record_hash="h1",
        slot_dt_local=SLOT_LOCAL,
        court_id=7,
        service_id=99,
        profile_name="roman",
        phase="window",
        booked_at_utc=BOOKED_AT,
    )
    base.update(overrides)
    return BookedSlot(**base)


# ---- Construction ----------------------------------------------------------


def test_missing_parent_dir_raises(tmp_path: Path) -> None:
    bad = tmp_path / "no-such-dir" / "bookings.jsonl"
    with pytest.raises(ValueError, match="parent directory"):
        FileBookingStore(bad)


def test_non_path_argument_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="path must be Path"):
        FileBookingStore(str(tmp_path / "x.jsonl"))  # type: ignore[arg-type]


def test_construct_ok_when_parent_exists(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    assert store.path == tmp_path / "bookings.jsonl"
    # Empty: file does not exist yet — but constructor doesn't create it.
    assert not (tmp_path / "bookings.jsonl").exists()


# ---- Append + persistence --------------------------------------------------


@pytest.mark.asyncio
async def test_append_creates_file_and_writes_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    s = _slot()
    await store.append(s)

    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert content.endswith("\n")
    line = content.strip()
    parsed = json.loads(line)
    assert parsed == s.to_dict()


@pytest.mark.asyncio
async def test_append_appends_multiple_lines(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    await store.append(_slot(record_id=1))
    await store.append(_slot(record_id=2))
    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["record_id"] == 1
    assert json.loads(lines[1])["record_id"] == 2


@pytest.mark.asyncio
async def test_concurrent_appends_serialised(tmp_path: Path) -> None:
    """Multiple concurrent append() must not interleave writes (lock + fsync)."""
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    slots = [_slot(record_id=i, record_hash=f"h{i}") for i in range(1, 11)]
    await asyncio.gather(*(store.append(s) for s in slots))

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 10
    record_ids = sorted(int(json.loads(line)["record_id"]) for line in lines)
    assert record_ids == list(range(1, 11))


# ---- Find / load -----------------------------------------------------------


@pytest.mark.asyncio
async def test_find_missing_file_returns_none(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    assert (
        await store.find(
            slot_dt_local=SLOT_LOCAL,
            court_ids=[7],
            service_id=99,
            profile_name="roman",
        )
        is None
    )


@pytest.mark.asyncio
async def test_find_after_append_returns_slot(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    s = _slot()
    await store.append(s)
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7],
        service_id=99,
        profile_name="roman",
    )
    assert found == s


@pytest.mark.asyncio
async def test_find_court_pool_or_match(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    await store.append(_slot(court_id=5))
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7, 5, 3],
        service_id=99,
        profile_name="roman",
    )
    assert found is not None
    assert found.court_id == 5


@pytest.mark.asyncio
async def test_find_no_match(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    await store.append(_slot(court_id=5))
    assert (
        await store.find(
            slot_dt_local=SLOT_LOCAL,
            court_ids=[1],
            service_id=99,
            profile_name="roman",
        )
        is None
    )


@pytest.mark.asyncio
async def test_find_empty_court_ids_returns_none(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    await store.append(_slot())
    assert (
        await store.find(
            slot_dt_local=SLOT_LOCAL,
            court_ids=[],
            service_id=99,
            profile_name="roman",
        )
        is None
    )


@pytest.mark.asyncio
async def test_find_skips_blank_lines(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    await store.append(_slot())
    # Inject blank lines around real data.
    raw = path.read_text(encoding="utf-8")
    path.write_text(f"\n\n{raw}\n\n", encoding="utf-8")
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7],
        service_id=99,
        profile_name="roman",
    )
    assert found is not None


@pytest.mark.asyncio
async def test_find_skips_invalid_json(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    await store.append(_slot())
    # Append junk line.
    with open(path, "a", encoding="utf-8") as f:
        f.write("this is not json\n")
    # Should still find the valid record.
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7],
        service_id=99,
        profile_name="roman",
    )
    assert found is not None


@pytest.mark.asyncio
async def test_find_skips_non_object_json(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    await store.append(_slot())
    with open(path, "a", encoding="utf-8") as f:
        f.write("[1, 2, 3]\n")  # JSON array, not object
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7],
        service_id=99,
        profile_name="roman",
    )
    assert found is not None


@pytest.mark.asyncio
async def test_find_skips_invalid_dict(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    await store.append(_slot())
    with open(path, "a", encoding="utf-8") as f:
        # Missing required fields.
        f.write(json.dumps({"foo": "bar"}) + "\n")
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7],
        service_id=99,
        profile_name="roman",
    )
    assert found is not None


@pytest.mark.asyncio
async def test_all_for_profile_after_append(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    await store.append(_slot(record_id=1))
    await store.append(_slot(record_id=2, profile_name="other"))
    await store.append(_slot(record_id=3))
    res = await store.all_for_profile("roman")
    assert {s.record_id for s in res} == {1, 3}


@pytest.mark.asyncio
async def test_all_for_profile_empty_when_no_match(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    await store.append(_slot(record_id=1))
    assert await store.all_for_profile("nobody") == []


@pytest.mark.asyncio
async def test_all_for_profile_missing_file(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    assert await store.all_for_profile("roman") == []


# ---- Round-trip: write, re-open, re-find -----------------------------------


@pytest.mark.asyncio
async def test_persists_across_store_instances(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    s = _slot()
    store_a = FileBookingStore(path)
    await store_a.append(s)

    store_b = FileBookingStore(path)
    found = await store_b.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7],
        service_id=99,
        profile_name="roman",
    )
    assert found == s


@pytest.mark.asyncio
async def test_append_writes_utf8_unicode(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    # profile_name regex forbids non-ASCII; record_hash может содержать любые
    # printable. Test with hash containing non-ASCII to confirm UTF-8 writes.
    await store.append(_slot(record_hash="hash-кириллица-👍"))
    raw = path.read_text(encoding="utf-8")
    assert "кириллица" in raw
    assert "👍" in raw


@pytest.mark.asyncio
async def test_path_property_exposed(tmp_path: Path) -> None:
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)
    assert store.path == path


# ---- find / all_for_profile across courts pool -----------------------------


@pytest.mark.asyncio
async def test_court_pool_first_match_returned(tmp_path: Path) -> None:
    store = FileBookingStore(tmp_path / "bookings.jsonl")
    await store.append(_slot(record_id=10, court_id=5))
    await store.append(_slot(record_id=11, court_id=7))
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7, 5],
        service_id=99,
        profile_name="roman",
    )
    assert found is not None
    # First record in file order matches court_id=5 (first appended).
    assert found.record_id == 10


# ---- fsync invoked (best-effort observable via file existence + size) ------


@pytest.mark.asyncio
async def test_fsync_called_after_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Confirm os.fsync is called as part of the append path."""
    path = tmp_path / "bookings.jsonl"
    store = FileBookingStore(path)

    fsync_calls: list[int] = []
    real_fsync = os.fsync

    def _spy(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(
        "tennis_booking.persistence.store.os.fsync", _spy
    )
    await store.append(_slot())
    assert len(fsync_calls) == 1
