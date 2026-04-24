from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from tennis_booking.common.tz import ALMATY
from tennis_booking.persistence.models import SCHEMA_VERSION, BookedSlot
from tennis_booking.persistence.store import MemoryBookingStore

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


@pytest.mark.asyncio
async def test_empty_find_returns_none() -> None:
    store = MemoryBookingStore()
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
async def test_append_and_find_exact_match() -> None:
    store = MemoryBookingStore()
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
async def test_find_with_court_pool_or_semantics() -> None:
    store = MemoryBookingStore()
    s = _slot(court_id=5)
    await store.append(s)
    # Search with court_ids=[7,5,3] — court 5 must match (OR).
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[7, 5, 3],
        service_id=99,
        profile_name="roman",
    )
    assert found == s


@pytest.mark.asyncio
async def test_find_no_match_court_returns_none() -> None:
    store = MemoryBookingStore()
    await store.append(_slot(court_id=5))
    assert (
        await store.find(
            slot_dt_local=SLOT_LOCAL,
            court_ids=[1, 2, 3],
            service_id=99,
            profile_name="roman",
        )
        is None
    )


@pytest.mark.asyncio
async def test_find_no_match_service_returns_none() -> None:
    store = MemoryBookingStore()
    await store.append(_slot())
    assert (
        await store.find(
            slot_dt_local=SLOT_LOCAL,
            court_ids=[7],
            service_id=88,
            profile_name="roman",
        )
        is None
    )


@pytest.mark.asyncio
async def test_find_no_match_profile_returns_none() -> None:
    store = MemoryBookingStore()
    await store.append(_slot())
    assert (
        await store.find(
            slot_dt_local=SLOT_LOCAL,
            court_ids=[7],
            service_id=99,
            profile_name="other",
        )
        is None
    )


@pytest.mark.asyncio
async def test_find_no_match_slot_dt_returns_none() -> None:
    store = MemoryBookingStore()
    await store.append(_slot())
    other_slot = datetime(2026, 4, 27, 18, 0, tzinfo=ALMATY)
    assert (
        await store.find(
            slot_dt_local=other_slot,
            court_ids=[7],
            service_id=99,
            profile_name="roman",
        )
        is None
    )


@pytest.mark.asyncio
async def test_empty_court_ids_returns_none() -> None:
    store = MemoryBookingStore()
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
async def test_all_for_profile() -> None:
    store = MemoryBookingStore()
    await store.append(_slot(record_id=1))
    await store.append(_slot(record_id=2, profile_name="other"))
    await store.append(_slot(record_id=3))
    by_roman = await store.all_for_profile("roman")
    assert {s.record_id for s in by_roman} == {1, 3}
    by_other = await store.all_for_profile("other")
    assert {s.record_id for s in by_other} == {2}


@pytest.mark.asyncio
async def test_first_match_returned_when_multiple() -> None:
    store = MemoryBookingStore()
    a = _slot(record_id=1, court_id=5)
    b = _slot(record_id=2, court_id=7)
    await store.append(a)
    await store.append(b)
    found = await store.find(
        slot_dt_local=SLOT_LOCAL,
        court_ids=[5, 7],
        service_id=99,
        profile_name="roman",
    )
    assert found == a
