"""Engine tests for the just-in-time atomic search check before fire.

PollAttempt.run() → poll_detected_bookable → _just_in_time_atomic_check()
filters the random court selection down to the subset Altegio currently says
is bookable. Cap=1 with N candidates becomes 1/1 instead of 1/N.
"""
from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioTransportError,
    BookableStaff,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.engine.attempt import AttemptConfig
from tennis_booking.engine.poll import PollAttempt, PollConfigData

from .conftest import (
    SERVICE_ID,
    SLOT,
    STAFF_ID,
    FakeClock,
)
from .test_poll_attempt import FakePollClient, as_client


def _slot(dt_local: datetime, *, is_bookable: bool, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=is_bookable, staff_id=staff_id)


def _bookable(staff_id: int, *, is_bookable: bool = True) -> BookableStaff:
    return BookableStaff(staff_id=staff_id, is_bookable=is_bookable)


def _make_attempt_config(**overrides: Any) -> AttemptConfig:
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


# ---- Filtered subset selection ---------------------------------------------


async def test_atomic_check_filters_subset_when_enabled() -> None:
    """Pool of 7 courts, atomic returns only 3 and 5 bookable, cap=1.
    Fired court_id MUST be 3 or 5 — never the other 5 not-bookable courts.
    """
    clock = _start_clock()
    pool = (1, 2, 3, 4, 5, 6, 7)
    booking = BookingResponse(record_id=42, record_hash="abc")
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking],
        staff_search_effects=[
            [
                _bookable(1, is_bookable=False),
                _bookable(2, is_bookable=False),
                _bookable(3, is_bookable=True),
                _bookable(4, is_bookable=False),
                _bookable(5, is_bookable=True),
                _bookable(6, is_bookable=False),
                _bookable(7, is_bookable=False),
            ]
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool, max_parallel_shots=1),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=True,
    )
    result = await poll.run()
    assert result.status == "won"
    assert len(fake.booking_calls) == 1
    fired_court_id = fake.booking_calls[0]["staff_id"]
    assert fired_court_id in {3, 5}
    # And atomic check happened exactly once (per fire).
    assert len(fake.search_staff_calls) == 1
    call = fake.search_staff_calls[0]
    assert call["datetime_local"] == SLOT
    assert call["service_id"] == SERVICE_ID
    # 200ms hard cap on the per-request timeout.
    assert call["timeout_s"] == 0.2


async def test_atomic_check_no_bookable_skips_fire() -> None:
    """If atomic returns ALL not-bookable, log no_bookable_courts_at_fire and
    return lost without firing."""
    clock = _start_clock()
    pool = (1, 2, 3)
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[],
        staff_search_effects=[
            [
                _bookable(1, is_bookable=False),
                _bookable(2, is_bookable=False),
                _bookable(3, is_bookable=False),
            ]
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool, max_parallel_shots=1),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=True,
    )

    async def driver() -> Any:
        return await poll.run()

    task = asyncio.create_task(driver())
    for _ in range(20):
        await asyncio.sleep(0)
    # Slot has not yet passed but no bookable; engine should `lost` once and
    # then keep polling until slot passes → timeout. We just check no fire.
    clock.advance(3 * 24 * 3600)
    result = await task
    assert result.status == "timeout"
    assert len(fake.booking_calls) == 0
    assert len(fake.search_staff_calls) >= 1


async def test_atomic_check_falls_back_to_blind_on_transport_failure() -> None:
    """Atomic check raises transport error → fall back to blind random across pool."""
    clock = _start_clock()
    pool = (1, 2, 3)
    booking = BookingResponse(record_id=42, record_hash="abc")
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking, booking, booking],
        staff_search_effects=[AltegioTransportError("ReadTimeout")],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=True,
    )
    result = await poll.run()
    assert result.status == "won"
    # Without atomic check filter, blind path fires on all pool members
    # (no max_parallel_shots set → full pool fan-out).
    fired_courts = {c["staff_id"] for c in fake.booking_calls}
    assert fired_courts == set(pool)


async def test_atomic_check_falls_back_to_blind_on_business_error() -> None:
    """Atomic check raises business error (e.g. invalid_filter) → fall back to blind."""
    clock = _start_clock()
    pool = (1, 2, 3)
    booking = BookingResponse(record_id=42, record_hash="abc")
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking, booking, booking],
        staff_search_effects=[
            AltegioBusinessError(code="bad_filter", message="x", http_status=422)
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=True,
    )
    result = await poll.run()
    assert result.status == "won"
    fired_courts = {c["staff_id"] for c in fake.booking_calls}
    assert fired_courts == set(pool)


async def test_atomic_check_disabled_via_flag_uses_blind() -> None:
    """Feature flag off → atomic check NOT called, blind random applies."""
    clock = _start_clock()
    pool = (1, 2, 3)
    booking = BookingResponse(record_id=42, record_hash="abc")
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking, booking, booking],
        staff_search_effects=[],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=False,
    )
    result = await poll.run()
    assert result.status == "won"
    assert len(fake.search_staff_calls) == 0
    fired_courts = {c["staff_id"] for c in fake.booking_calls}
    assert fired_courts == set(pool)


async def test_atomic_check_timeout_falls_back_to_blind() -> None:
    """If the atomic check coroutine itself takes longer than the 200ms ceiling,
    the engine aborts via asyncio.wait_for and falls back to blind random.
    """
    clock = _start_clock()
    pool = (1, 2, 3)
    booking = BookingResponse(record_id=42, record_hash="abc")

    async def slow_atomic() -> list[BookableStaff]:
        # Real wall clock sleep > 200ms; FakeClock won't shorten asyncio.wait_for.
        await asyncio.sleep(0.5)
        return [_bookable(c) for c in pool]

    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking, booking, booking],
        staff_search_effects=[slow_atomic],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=True,
    )
    result = await poll.run()
    assert result.status == "won"
    fired_courts = {c["staff_id"] for c in fake.booking_calls}
    assert fired_courts == set(pool)


async def test_atomic_check_intersected_with_pool() -> None:
    """Atomic returns extra staff_ids not in our pool — those are ignored;
    intersection only with configured pool counts as eligible.
    """
    clock = _start_clock()
    pool = (10, 20, 30)
    booking = BookingResponse(record_id=42, record_hash="abc")
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[booking],
        staff_search_effects=[
            [
                _bookable(10, is_bookable=False),
                _bookable(20, is_bookable=True),
                _bookable(30, is_bookable=False),
                # Not in our pool — must be filtered out.
                _bookable(99, is_bookable=True),
                _bookable(100, is_bookable=True),
            ]
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=pool, max_parallel_shots=1),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        rng=random.Random(0),
        atomic_search_before_fire_enabled=True,
    )
    result = await poll.run()
    assert result.status == "won"
    fired_court_id = fake.booking_calls[0]["staff_id"]
    # Only staff_id=20 from our pool is bookable; 99/100 must NOT leak.
    assert fired_court_id == 20
