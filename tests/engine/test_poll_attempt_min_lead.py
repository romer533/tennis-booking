"""Tests for `min_lead_time_hours` guard on `PollAttempt._fire_shots`.

When poll detects a bookable slot within `min_lead_time_hours` of now, it must
return status="error" / business_code="too_close_to_slot" without firing any
create_booking POST. Detection (search_timeslots) still happens — the guard
sits between detection and fan-out fire.
"""
from __future__ import annotations

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
        import asyncio

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
        import asyncio

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


def _bookable_slot(dt_local: datetime, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=True, staff_id=staff_id)


def _make_cfg(*, slot: datetime, min_lead_time_hours: float = 0.0) -> AttemptConfig:
    return AttemptConfig(
        slot_dt_local=slot,
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77026473809",
        profile_name="roman",
        min_lead_time_hours=min_lead_time_hours,
    )


# ---- Tests ----------------------------------------------------------------


async def test_poll_bookable_within_guard_returns_error_no_fire() -> None:
    """Slot 1h away, min_lead=2 → poll detects bookable but skips fire."""
    # Clock is 1h before slot.
    slot = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=1)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[[_bookable_slot(slot)]],
        booking_effects=[],  # No POST should happen.
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=10, start_offset_days=1),
        as_client(fake),
        clock,
    )
    result = await poll.run()

    assert result.status == "error"
    assert result.business_code == "too_close_to_slot"
    assert result.shots_fired == 0
    # Search happened (detection) but no booking POST.
    assert len(fake.search_calls) == 1
    assert len(fake.booking_calls) == 0


async def test_poll_bookable_outside_guard_fires_normally() -> None:
    """Slot 5h away, min_lead=2 → poll fires normally on detection."""
    slot = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=5)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[[_bookable_slot(slot)]],
        booking_effects=[BookingResponse(record_id=42, record_hash="h")],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=10, start_offset_days=1),
        as_client(fake),
        clock,
    )
    result = await poll.run()

    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 42
    assert len(fake.booking_calls) == 1


async def test_poll_guard_disabled_zero_fires_close_slot() -> None:
    """min_lead=0 → poll fires even when slot is very close."""
    slot = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(minutes=10)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[[_bookable_slot(slot)]],
        booking_effects=[BookingResponse(record_id=99, record_hash="h")],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=0.0),
        PollConfigData(interval_s=10, start_offset_days=1),
        as_client(fake),
        clock,
    )
    result = await poll.run()

    assert result.status == "won"
    assert len(fake.booking_calls) == 1


async def test_poll_per_booking_override_higher_than_default() -> None:
    """Per-booking override 4h; slot 3h away → guard fires."""
    slot = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
    initial_utc = (slot - timedelta(hours=3)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[[_bookable_slot(slot)]],
        booking_effects=[],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=4.0),
        PollConfigData(interval_s=10, start_offset_days=1),
        as_client(fake),
        clock,
    )
    result = await poll.run()

    assert result.status == "error"
    assert result.business_code == "too_close_to_slot"
    assert len(fake.booking_calls) == 0


async def test_poll_guard_just_under_threshold() -> None:
    """Slot 1.99h away, min_lead=2 → guard fires (strict less-than)."""
    slot = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
    # 1.99h before slot.
    initial_utc = (slot - timedelta(hours=1.99)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)

    fake = FakePollClient(
        search_effects=[[_bookable_slot(slot)]],
        booking_effects=[],
    )
    poll = PollAttempt(
        _make_cfg(slot=slot, min_lead_time_hours=2.0),
        PollConfigData(interval_s=10, start_offset_days=1),
        as_client(fake),
        clock,
    )
    result = await poll.run()

    assert result.status == "error"
    assert result.business_code == "too_close_to_slot"
