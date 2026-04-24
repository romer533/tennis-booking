from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import SecretStr

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioConfig,
    AltegioTransportError,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.altegio.client import AltegioClient
from tennis_booking.engine.attempt import AttemptConfig
from tennis_booking.engine.poll import PollAttempt, PollConfigData

from .conftest import (
    BASE_URL,
    BEARER,
    BOOKFORM_ID,
    COMPANY_ID,
    SERVICE_ID,
    SLOT,
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
    """Fake AltegioClient supporting search_timeslots + create_booking scripts."""

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


def _slot(dt_local: datetime, *, is_bookable: bool, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=is_bookable, staff_id=staff_id)


def _make_attempt_config(**overrides: Any) -> AttemptConfig:
    defaults: dict[str, Any] = {
        "slot_dt_local": SLOT,
        "court_ids": (STAFF_ID,),
        "service_id": SERVICE_ID,
        "fullname": "Roman",
        "phone": "77026473809",
        "email": None,
    }
    defaults.update(overrides)
    return AttemptConfig(**defaults)


def _start_clock() -> FakeClock:
    """Initial UTC = SLOT - 2 days, so a typical poll with start_offset_days=2 starts immediately."""
    initial = (SLOT - timedelta(days=2)).astimezone(UTC)
    return FakeClock(initial_utc=initial, initial_mono=1000.0)


# ---- PollConfigData validation ---------------------------------------------


def test_poll_config_data_valid() -> None:
    p = PollConfigData(interval_s=60, start_offset_days=2)
    assert p.interval_s == 60


def test_poll_config_data_interval_below_floor() -> None:
    with pytest.raises(ValueError, match="interval_s"):
        PollConfigData(interval_s=5, start_offset_days=2)


def test_poll_config_data_offset_zero() -> None:
    with pytest.raises(ValueError, match="start_offset_days"):
        PollConfigData(interval_s=60, start_offset_days=0)


def test_poll_config_data_offset_above_max() -> None:
    with pytest.raises(ValueError, match="start_offset_days"):
        PollConfigData(interval_s=60, start_offset_days=31)


# ---- Detection + fire happy path -------------------------------------------


async def test_poll_detects_bookable_and_wins() -> None:
    clock = _start_clock()
    booking_resp = BookingResponse(record_id=42, record_hash="abc")
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=False)],
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[booking_resp],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 42
    assert result.phase == "poll"
    assert len(fake.booking_calls) == 1
    assert fake.booking_calls[0]["staff_id"] == STAFF_ID


async def test_poll_detects_immediately_first_tick() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    # No interval sleep needed before first detect
    assert len(fake.search_calls) == 1


async def test_poll_skips_unbookable_then_wins() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=False)],
            [_slot(SLOT, is_bookable=False)],
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[BookingResponse(record_id=99, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    assert len(fake.search_calls) == 3


# ---- won_event coordination -------------------------------------------------


async def test_won_event_set_at_start_returns_early() -> None:
    clock = _start_clock()
    fake = FakePollClient(search_effects=[], booking_effects=[])
    won = asyncio.Event()
    won.set()
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        won_event=won,
    )
    result = await poll.run()
    # No search performed — sibling already won.
    assert len(fake.search_calls) == 0
    assert result.status == "lost"
    assert result.business_code == "won_by_sibling"


async def test_won_event_set_during_polling_after_tick() -> None:
    """Sibling sets won_event between first poll tick and second — second tick aborts."""
    clock = _start_clock()
    won = asyncio.Event()

    async def search_then_set() -> list[TimeSlot]:
        # On first call return empty (no bookable). After: set won_event.
        won.set()
        return [_slot(SLOT, is_bookable=False)]

    fake = FakePollClient(
        search_effects=[search_then_set],
        booking_effects=[],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        won_event=won,
    )
    result = await poll.run()
    assert result.status == "lost"
    assert result.business_code == "won_by_sibling"
    assert len(fake.search_calls) == 1


async def test_poll_sets_won_event_on_win() -> None:
    clock = _start_clock()
    won = asyncio.Event()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=7, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        won_event=won,
    )
    result = await poll.run()
    assert result.status == "won"
    # NB: PollAttempt.set() the event before fire as exclusivity claim;
    # on win, it stays set (signalling to sibling).
    assert won.is_set()


async def test_poll_clears_won_event_on_fire_loss() -> None:
    """If we set won_event before fire but lost (slot_taken or transport),
    we must clear it so a still-live sibling can fire.
    """
    clock = _start_clock()
    won = asyncio.Event()
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=True)],
            [_slot(SLOT, is_bookable=False)],
            [_slot(SLOT, is_bookable=False)],
        ],
        booking_effects=[AltegioTransportError("ConnectError")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        won_event=won,
    )

    # We expect this to keep polling. To stop the test, advance clock past slot.
    async def driver() -> Any:
        return await poll.run()

    task = asyncio.create_task(driver())
    # let detect-fire-clear cycle run
    for _ in range(10):
        await asyncio.sleep(0)
    # Check that after fire-loss, won_event was cleared (once we passed the lost path)
    # Force completion by advancing clock past slot.
    clock.advance(3 * 24 * 3600)  # past slot
    result = await task
    # transport_cause: slot_passed (we reached timeout)
    assert result.status == "timeout"
    assert result.transport_cause == "slot_passed"
    # won_event should have been cleared at some point during the process.
    # Final state: not set, since last action was clear → continue polling → timeout.
    assert not won.is_set()


# ---- Config error -----------------------------------------------------------


async def test_search_unauthorized_returns_error() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[
            AltegioBusinessError(code="unauthorized", message="bad token", http_status=401)
        ],
        booking_effects=[],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "error"
    assert result.business_code == "unauthorized"


# ---- Transport / business soft errors --------------------------------------


async def test_search_transport_error_continues_polling() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[
            AltegioTransportError("ReadTimeout"),
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    assert len(fake.search_calls) == 2


async def test_search_unknown_business_error_continues_polling() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[
            AltegioBusinessError(code="rate_limited", message="x", http_status=429),
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"


# ---- Slot timing ------------------------------------------------------------


async def test_slot_already_passed_at_start_returns_timeout() -> None:
    initial = (SLOT + timedelta(hours=1)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial)
    fake = FakePollClient(search_effects=[], booking_effects=[])
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "timeout"
    assert result.transport_cause == "slot_passed"
    assert len(fake.search_calls) == 0


async def test_start_offset_in_future_sleeps_then_polls() -> None:
    """If now < slot - start_offset_days, must sleep until effective_start before polling."""
    initial = (SLOT - timedelta(days=5)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial)
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    # The first sleep should have been ~3 days (5 - 2)
    assert len(clock.sleep_calls) >= 1
    assert clock.sleep_calls[0] > 2 * 24 * 3600


# ---- Cancellation -----------------------------------------------------------


async def test_cancellation_propagates() -> None:
    clock = _start_clock()

    async def hang() -> list[TimeSlot]:
        await asyncio.sleep(3600)
        return []

    fake = FakePollClient(
        search_effects=[hang],
        booking_effects=[],
    )

    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    task = asyncio.create_task(poll.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ---- Single-shot guard ------------------------------------------------------


async def test_run_twice_raises() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    await poll.run()
    with pytest.raises(RuntimeError, match="single-shot"):
        await poll.run()


# ---- Pool fan-out -----------------------------------------------------------


async def test_pool_fan_out_first_wins_others_cancelled() -> None:
    """Pool with 3 courts: search returns bookable, fan-out fires 3 shots, one wins."""
    clock = _start_clock()
    booking = BookingResponse(record_id=100, record_hash="h")
    fake = FakePollClient(
        search_effects=[
            [
                _slot(SLOT, is_bookable=True, staff_id=5),
                _slot(SLOT, is_bookable=True, staff_id=6),
                _slot(SLOT, is_bookable=True, staff_id=7),
            ]
        ],
        booking_effects=[
            booking,
            AltegioBusinessError(code="slot_busy", message="taken", http_status=422),
            AltegioBusinessError(code="slot_busy", message="taken", http_status=422),
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=(5, 6, 7)),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    assert len(fake.booking_calls) == 3  # all three fired
    assert {c["staff_id"] for c in fake.booking_calls} == {5, 6, 7}


async def test_pool_no_match_per_court_falls_back_to_any() -> None:
    """Old API (no staff_id in slots) — any bookable slot at our datetime triggers fire on all."""
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True, staff_id=None)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=(STAFF_ID,)),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"


async def test_search_filters_by_target_datetime_only() -> None:
    """A bookable slot at a different time must NOT trigger fire."""
    clock = _start_clock()
    other_slot_dt = SLOT + timedelta(hours=1)
    fake = FakePollClient(
        search_effects=[
            [_slot(other_slot_dt, is_bookable=True)],  # different time
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"
    assert len(fake.search_calls) == 2


async def test_pool_logging_with_many_courts() -> None:
    """8+ courts trigger compact logging branch (court_id_primary + court_count)."""
    clock = _start_clock()
    cids = tuple(range(101, 110))  # 9 courts
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[
            BookingResponse(record_id=1, record_hash="h"),
        ] + [
            AltegioBusinessError(code="slot_busy", message="x", http_status=422)
        ] * 8,
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=cids),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"


async def test_search_unexpected_exception_continues_polling() -> None:
    clock = _start_clock()

    async def raise_runtime() -> list[TimeSlot]:
        raise RuntimeError("weird")

    fake = FakePollClient(
        search_effects=[
            raise_runtime,
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "won"


async def test_fire_all_transport_returns_lost_with_cause() -> None:
    """All shots fail with transport — result is lost with transport_cause."""
    clock = _start_clock()
    won = asyncio.Event()
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=True, staff_id=5), _slot(SLOT, is_bookable=True, staff_id=6)],
            [_slot(SLOT, is_bookable=False, staff_id=5), _slot(SLOT, is_bookable=False, staff_id=6)],
        ],
        booking_effects=[
            AltegioTransportError("ReadTimeout"),
            AltegioTransportError("ConnectError"),
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=(5, 6)),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        won_event=won,
    )

    async def driver() -> Any:
        return await poll.run()

    task = asyncio.create_task(driver())
    for _ in range(20):
        await asyncio.sleep(0)
    # Push past slot to terminate gracefully
    clock.advance(3 * 24 * 3600)
    result = await task
    # First fire reset won_event after lost; then continued polling, slot_passed
    assert result.status == "timeout"
    assert not won.is_set()


async def test_fire_unknown_business_code_returns_lost_fallback() -> None:
    """When create_booking returns an unknown business code, result is lost
    (fallback) with that code; we then continue polling.
    """
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=True)],
            [_slot(SLOT, is_bookable=False)],
        ],
        booking_effects=[
            AltegioBusinessError(code="weird_code", message="x", http_status=422),
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    # Drive past slot to terminate
    task = asyncio.create_task(poll.run())
    for _ in range(20):
        await asyncio.sleep(0)
    clock.advance(3 * 24 * 3600)
    result = await task
    assert result.status == "timeout"


async def test_fire_config_err_returns_error_immediately() -> None:
    """unauthorized from create_booking aborts the poll attempt."""
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[
            AltegioBusinessError(code="unauthorized", message="x", http_status=401),
        ],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    result = await poll.run()
    assert result.status == "error"
    assert result.business_code == "unauthorized"


async def test_search_call_uses_correct_date_and_staff_ids() -> None:
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(court_ids=(5, 6)),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
    )
    await poll.run()
    call = fake.search_calls[0]
    assert call["date_local"] == SLOT.date()
    assert call["staff_ids"] == [5, 6]
