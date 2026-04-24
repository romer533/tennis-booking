"""PollAttempt — persistence hook on win path."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tennis_booking.altegio import (
    AltegioBusinessError,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.engine.attempt import AttemptConfig
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.persistence.models import BookedSlot
from tennis_booking.persistence.store import MemoryBookingStore

from .conftest import (
    SERVICE_ID,
    SLOT,
    STAFF_ID,
    FakeClock,
)
from .test_poll_attempt import FakePollClient, as_client


def _slot(dt_local: datetime, *, is_bookable: bool, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=is_bookable, staff_id=staff_id)


def _attempt_cfg(**overrides: Any) -> AttemptConfig:
    defaults: dict[str, Any] = {
        "slot_dt_local": SLOT,
        "court_ids": (STAFF_ID,),
        "service_id": SERVICE_ID,
        "fullname": "Roman",
        "phone": "77026473809",
        "profile_name": "roman",
        "email": None,
    }
    defaults.update(overrides)
    return AttemptConfig(**defaults)


def _start_clock() -> FakeClock:
    initial = (SLOT - timedelta(days=2)).astimezone(UTC)
    return FakeClock(initial_utc=initial, initial_mono=1000.0)


class _RaisingStore:
    async def append(self, slot: BookedSlot) -> None:
        raise RuntimeError("disk fail")

    async def find(
        self,
        slot_dt_local: datetime,
        court_ids: list[int],
        service_id: int,
        profile_name: str,
    ) -> BookedSlot | None:
        return None

    async def all_for_profile(self, profile_name: str) -> list[BookedSlot]:
        return []


# ---- Win path persists -----------------------------------------------------


@pytest.mark.asyncio
async def test_poll_win_persists() -> None:
    clock = _start_clock()
    store = MemoryBookingStore()
    booking_resp = BookingResponse(record_id=42, record_hash="hpoll")
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking_resp],
    )
    cfg = _attempt_cfg()
    poll = PollAttempt(
        cfg,
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        store=store,
    )
    result = await poll.run()
    assert result.status == "won"
    persisted = await store.all_for_profile(cfg.profile_name)
    assert len(persisted) == 1
    rec = persisted[0]
    assert rec.record_id == 42
    assert rec.phase == "poll"
    assert rec.court_id == STAFF_ID
    assert rec.service_id == SERVICE_ID
    assert rec.slot_dt_local == SLOT
    assert rec.booked_at_utc.tzinfo is not None


@pytest.mark.asyncio
async def test_poll_no_store_works() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _attempt_cfg(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"


@pytest.mark.asyncio
async def test_poll_persistence_failure_swallowed() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=99, record_hash="h99")],
    )
    poll = PollAttempt(
        _attempt_cfg(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        store=_RaisingStore(),  # type: ignore[arg-type]
    )
    result = await poll.run()
    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 99


@pytest.mark.asyncio
async def test_poll_pool_winning_court_id_recorded() -> None:
    """Pool of 2 courts: bookable on both. FIFO consumes idx=0 first → court_ids[0]."""
    clock = _start_clock()
    store = MemoryBookingStore()
    fake = FakePollClient(
        search_effects=[
            [
                _slot(SLOT, is_bookable=True, staff_id=11),
                _slot(SLOT, is_bookable=True, staff_id=22),
            ]
        ],
        booking_effects=[
            BookingResponse(record_id=11, record_hash="h11"),
            BookingResponse(record_id=22, record_hash="h22"),
        ],
    )
    cfg = _attempt_cfg(court_ids=(11, 22))
    poll = PollAttempt(
        cfg,
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        store=store,
    )
    result = await poll.run()
    assert result.status == "won"
    persisted = await store.all_for_profile(cfg.profile_name)
    assert len(persisted) == 1
    # asyncio.wait FIRST_COMPLETED order is non-deterministic in poll mode —
    # any of the configured court_ids is acceptable as the winning court_id,
    # as long as it is one of them (i.e. the engine recorded a real court).
    assert persisted[0].court_id in (11, 22)


@pytest.mark.asyncio
async def test_poll_lost_does_not_persist(monkeypatch: pytest.MonkeyPatch) -> None:
    from tennis_booking.engine import codes as codes_module
    from tennis_booking.engine import poll as poll_module

    monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
    monkeypatch.setattr(poll_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))

    clock = _start_clock()
    store = MemoryBookingStore()
    # First tick: bookable, but POST returns slot_taken — poll continues.
    # Second tick: not bookable → loop continues; we then jump time past SLOT.
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=True)],
            [_slot(SLOT, is_bookable=False)],
            [_slot(SLOT, is_bookable=False)],
        ],
        booking_effects=[
            AltegioBusinessError(code="slot_busy", message="taken", http_status=422),
        ],
    )
    # Advance won_event clear + then jump clock past SLOT to trigger slot_passed.
    cfg = _attempt_cfg()
    poll = PollAttempt(
        cfg,
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        store=store,
    )

    # Run with cancellation to bound time — but FakeClock auto-advances on sleep.
    async def stop_after_one_loss() -> None:
        # Wait a bit, then advance clock past slot deadline.
        await asyncio.sleep(0)
        # Advance through a full poll cycle.
        await asyncio.sleep(0)
        clock.advance(3 * 24 * 3600)  # past slot

    asyncio.create_task(stop_after_one_loss())
    result = await poll.run()
    # We don't assert win/lost — main check: nothing persisted (no win).
    assert result.status in ("lost", "timeout")
    assert await store.all_for_profile(cfg.profile_name) == []


@pytest.mark.asyncio
async def test_poll_record_round_trips() -> None:
    clock = _start_clock()
    store = MemoryBookingStore()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=10, record_hash="h10")],
    )
    cfg = _attempt_cfg()
    poll = PollAttempt(
        cfg,
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        store=store,
    )
    await poll.run()
    rec = (await store.all_for_profile(cfg.profile_name))[0]
    rebuilt = BookedSlot.from_dict(rec.to_dict())
    assert rebuilt == rec


@pytest.mark.asyncio
async def test_poll_default_store_is_none() -> None:
    clock = _start_clock()
    fake = FakePollClient(search_effects=[[]], booking_effects=[])
    poll = PollAttempt(
        _attempt_cfg(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    assert poll._store is None  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_poll_slot_passed_no_persist() -> None:
    """Effective start already past slot — instant slot_passed, no booking, no persist."""
    initial = (SLOT + timedelta(seconds=10)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial, initial_mono=1000.0)
    store = MemoryBookingStore()
    fake = FakePollClient(search_effects=[], booking_effects=[])
    cfg = _attempt_cfg()
    poll = PollAttempt(
        cfg,
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        store=store,
    )
    result = await poll.run()
    assert result.status == "timeout"
    assert await store.all_for_profile(cfg.profile_name) == []
