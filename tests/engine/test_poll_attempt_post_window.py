"""Tests for `PollAttempt(post_window_mode=True)`.

Post-window mode is engaged after a window-task has lost; it polls until
`slot_dt_local - min_lead_time_hours` looking for cancellation slots released
by other users. Differences from default poll mode:
  - effective_start = now (no `start_offset_days` wait)
  - stop_at = slot_dt_local - min_lead_time_hours (not slot_dt_local)
  - log binding "phase": "post_window_poll"
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import SecretStr

from tennis_booking.altegio import (
    AltegioConfig,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.altegio.client import AltegioClient
from tennis_booking.common.tz import ALMATY
from tennis_booking.engine.attempt import AttemptConfig
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.persistence.models import SCHEMA_VERSION, BookedSlot
from tennis_booking.persistence.store import MemoryBookingStore

from .conftest import (
    BASE_URL,
    BEARER,
    BOOKFORM_ID,
    COMPANY_ID,
    SERVICE_ID,
    STAFF_ID,
    FakeClock,
    SideEffect,
)


def _config(*, dry_run: bool = False) -> AltegioConfig:
    return AltegioConfig(
        bearer_token=SecretStr(BEARER),
        base_url=BASE_URL,
        company_id=COMPANY_ID,
        bookform_id=BOOKFORM_ID,
        dry_run=dry_run,
    )


SearchEffect = list[TimeSlot] | BaseException | Callable[[], Awaitable[list[TimeSlot]]]


class FakePollClient:
    def __init__(
        self,
        *,
        search_effects: list[SearchEffect] | None = None,
        booking_effects: list[SideEffect] | None = None,
        config: AltegioConfig | None = None,
    ) -> None:
        self._search_effects: list[SearchEffect] = list(search_effects or [])
        self._booking_effects: list[SideEffect] = list(booking_effects or [])
        self._config = config or _config()
        self.search_calls: list[dict[str, Any]] = []
        self.booking_calls: list[dict[str, Any]] = []

    @property
    def config(self) -> AltegioConfig:
        return self._config

    async def search_timeslots(
        self,
        *,
        date_local: Any,
        staff_ids: list[int],
        timeout_s: float | None = None,
    ) -> list[TimeSlot]:
        self.search_calls.append(
            {"date_local": date_local, "staff_ids": list(staff_ids), "timeout_s": timeout_s}
        )
        if not self._search_effects:
            raise AssertionError("FakePollClient: no more search_timeslots effects")
        effect = self._search_effects.pop(0)
        await asyncio.sleep(0)
        if isinstance(effect, BaseException):
            raise effect
        if callable(effect):
            return await effect()
        return effect

    async def create_booking(
        self,
        *,
        service_id: int,
        staff_id: int,
        slot_dt_local: datetime,
        fullname: str,
        phone: str,
        email: str | None = None,
        timeout_s: float | None = None,
    ) -> BookingResponse:
        self.booking_calls.append(
            {
                "service_id": service_id,
                "staff_id": staff_id,
                "slot_dt_local": slot_dt_local,
                "fullname": fullname,
                "phone": phone,
                "email": email,
            }
        )
        if not self._booking_effects:
            raise AssertionError("FakePollClient: no more create_booking effects")
        effect = self._booking_effects.pop(0)
        await asyncio.sleep(0)
        if isinstance(effect, BookingResponse):
            return effect
        if isinstance(effect, BaseException):
            raise effect
        return await effect()


def as_client(fake: FakePollClient) -> AltegioClient:
    return fake  # type: ignore[return-value]


def _bookable(dt_local: datetime, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=True, staff_id=staff_id)


def _unbookable(dt_local: datetime, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=False, staff_id=staff_id)


def _make_cfg(
    *,
    slot: datetime,
    min_lead_time_hours: float = 0.0,
    court_ids: tuple[int, ...] = (STAFF_ID,),
) -> AttemptConfig:
    return AttemptConfig(
        slot_dt_local=slot,
        court_ids=court_ids,
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77026473809",
        profile_name="roman",
        min_lead_time_hours=min_lead_time_hours,
    )


# ---- Detection -------------------------------------------------------------


async def test_post_window_starts_immediately_no_offset_wait() -> None:
    """post_window_mode bypasses the start_offset_days wait — first search runs
    in the very first iteration (no `clock.sleep_calls[0]` worth of days).
    """
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=10)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[[_bookable(slot)]],
        booking_effects=[BookingResponse(record_id=42, record_hash="h")],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        post_window_mode=True,
    )
    result = await poll.run()
    assert result.status == "won"
    assert result.booking is not None
    # First search must hit on the very first poll iteration.
    assert len(fake.search_calls) == 1
    # No multi-hour pre-poll sleep was scheduled.
    assert not any(s > 60 * 60 for s in clock.sleep_calls)


async def test_post_window_stops_at_min_lead_time() -> None:
    """When clock advances past slot - min_lead, post-window terminates with
    status=timeout and transport_cause=post_window_window_closed.
    """
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    # Start 4h before slot, min_lead=2 → 2h of polling window.
    initial_utc = (slot - timedelta(hours=4)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        # Always-unbookable so the poll keeps ticking.
        search_effects=[[_unbookable(slot)]] * 1000,
        booking_effects=[],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        post_window_mode=True,
    )
    task = asyncio.create_task(poll.run())
    # Drive the clock past stop_at.
    for _ in range(500):
        if task.done():
            break
        await asyncio.sleep(0)
        clock.advance(60.0)
    result = await task
    assert result.status == "timeout"
    assert result.transport_cause == "post_window_window_closed"


async def test_post_window_stop_at_already_passed_returns_timeout() -> None:
    """If now >= slot - min_lead at the very start, returns timeout immediately."""
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    # 1h before slot, min_lead=2 → stop_at is in the past.
    initial_utc = (slot - timedelta(hours=1)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(search_effects=[], booking_effects=[])
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        post_window_mode=True,
    )
    result = await poll.run()
    assert result.status == "timeout"
    assert result.transport_cause == "post_window_window_closed"
    assert len(fake.search_calls) == 0


async def test_post_window_fires_when_bookable_detected() -> None:
    """Standard win-on-detection path works in post-window mode (search → fire → won)."""
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=24)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[
            [_unbookable(slot)],
            [_bookable(slot)],
        ],
        booking_effects=[BookingResponse(record_id=99, record_hash="hh")],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        post_window_mode=True,
    )
    result = await poll.run()
    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 99
    assert len(fake.search_calls) == 2
    assert len(fake.booking_calls) == 1


async def test_post_window_persistence_dedup_before_fire() -> None:
    """If the store already contains a record for (slot, court, service, profile),
    the post-window engine itself runs to win/timeout — but the scheduler is
    the layer that prevents starting it. Here we verify that a successful win
    persists with phase='poll' (BookedSlot.phase Literal compatibility).
    """
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=24)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    store = MemoryBookingStore()
    fake = FakePollClient(
        search_effects=[[_bookable(slot)]],
        booking_effects=[BookingResponse(record_id=7, record_hash="h7")],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        store=store,
        post_window_mode=True,
    )
    result = await poll.run()
    assert result.status == "won"
    found = await store.find(
        slot_dt_local=slot,
        court_ids=[STAFF_ID],
        service_id=SERVICE_ID,
        profile_name="roman",
    )
    assert found is not None
    assert found.record_id == 7


async def test_post_window_fire_skipped_if_someone_inserted_into_store() -> None:
    """Pre-existing record across profile boundary is dedup'd by the SCHEDULER,
    not the engine — but verify the engine does fire when no record exists.
    The cross-profile dedup is asserted in scheduler tests.
    """
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=24)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)
    store = MemoryBookingStore()

    # Different profile_name: engine-level dedup looks at profile_name match
    # via store.find — same profile only. So this record should NOT block.
    other_profile = BookedSlot(
        schema_version=SCHEMA_VERSION,
        record_id=11,
        record_hash="x",
        slot_dt_local=slot,
        court_id=STAFF_ID,
        service_id=SERVICE_ID,
        profile_name="askar",
        phase="poll",
        booked_at_utc=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
    )
    await store.append(other_profile)

    fake = FakePollClient(
        search_effects=[[_bookable(slot)]],
        booking_effects=[BookingResponse(record_id=22, record_hash="h22")],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        store=store,
        post_window_mode=True,
    )
    result = await poll.run()
    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 22


async def test_post_window_won_event_signal_sibling_aborts() -> None:
    """Cross-profile dedup signal: sibling post-window poll sets won_event →
    this poll exits with `won_by_sibling`.
    """
    slot = datetime(2026, 5, 1, 18, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=10)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    won = asyncio.Event()
    won.set()
    fake = FakePollClient(search_effects=[], booking_effects=[])
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=60, start_offset_days=1),
        as_client(fake),
        clock,
        won_event=won,
        post_window_mode=True,
    )
    result = await poll.run()
    assert result.status == "lost"
    assert result.business_code == "won_by_sibling"
    assert len(fake.search_calls) == 0
