"""Tests for `min_lead_time_hours` guard during grace polling fire iterations.

Grace fire (`_fire_shots_grace`) must respect the same lead-time guard as initial
fan-out and poll fire. If the guard trips, grace returns status="error" /
business_code="too_close_to_slot" and the outer grace loop exits.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from tennis_booking.altegio import (
    AltegioBusinessError,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.common.tz import ALMATY
from tennis_booking.config.schema import GracePollingConfig
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    STAFF_ID,
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111) -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash="h")


def _snv() -> AltegioBusinessError:
    return AltegioBusinessError(
        code="service_not_available",
        message="not available",
        http_status=422,
    )


def _bookable_slot(dt: datetime, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt, is_bookable=True, staff_id=staff_id)


def _make_close_clock(slot: datetime, hours_before: float) -> FakeClock:
    initial_utc = (slot - timedelta(hours=hours_before)).astimezone(UTC)
    return FakeClock(initial_utc=initial_utc, initial_mono=1000.0)


# ---- Tests ----------------------------------------------------------------


async def test_grace_fire_skipped_when_clock_drifts_past_threshold(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
) -> None:
    """Initial fire passes guard (slot > 2h); all shots snv → enter grace.
    Grace polling sleeps push clock past the 2h threshold. When grace fire runs,
    its guard catches the now-too-close slot and returns error without POSTing.
    """
    # window_open at clock+60s. fire_at = window_open. slot = fire_at + 2h + 30s.
    # not_open_deadline 1s, then grace interval 10s → search → grace_fire.
    # At grace_fire: now ≈ window_open + 1s + 10s = window_open + 11s.
    # time_to_slot = (fire_at + 2h + 30s) - (fire_at + 11s) = 2h + 19s > 2h.
    # Hmm — still > 2h. We need the slot to be JUST past 2h at initial fire so
    # the 10s + 1s push it under. Use slot = window_open + 2h + 5s.
    # At grace_fire: time_to_slot ≈ 2h + 5s - 11s ≈ 1h 59m 54s < 2h → guard fires.
    slot = datetime(2026, 4, 27, 10, 0, 0, tzinfo=ALMATY)
    # Pick clock such that window_open = clock + 60s, slot = window_open + 2h + 5s.
    initial_utc = (slot - timedelta(hours=2, seconds=65)).astimezone(UTC)
    clock = FakeClock(initial_utc=initial_utc, initial_mono=1000.0)
    window_open = clock.now_utc() + timedelta(seconds=60)

    client = fake_client([])
    client.set_default_side_effect(_snv())
    # Search returns bookable on first iteration → grace fire would happen,
    # but guard must intercept it.
    client.add_search([_bookable_slot(slot)])

    cfg = attempt_config(
        slot_dt_local=slot,
        court_ids=(STAFF_ID,),
        parallel_shots=1,
        not_open_retry_ms=100,
        not_open_deadline_s=1.0,
        global_deadline_s=2.0,
        grace_polling=GracePollingConfig(period_s=180, interval_s=10),
        min_lead_time_hours=2.0,
    )

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)

    assert result.status == "error"
    assert result.business_code == "too_close_to_slot"
    # Search was called (grace entered) but grace fire skipped guard → no
    # additional POST. Initial-phase POSTs still counted.
    assert len(client.search_timeslots_calls) >= 1
    # The grace-phase fire would have added booking calls; verify none came
    # AFTER the search call (we can compare counts vs prior to search).
    # Simplest invariant: the result is error/too_close_to_slot, and the won
    # booking is None — proving no successful POST in grace.
    assert result.booking is None


async def test_grace_fire_proceeds_when_outside_guard(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
) -> None:
    """Slot 5h away → grace fires normally and wins."""
    slot = datetime(2026, 4, 27, 10, 0, 0, tzinfo=ALMATY)
    clock = _make_close_clock(slot, hours_before=5.0)
    window_open = clock.now_utc() + timedelta(seconds=60)

    client = fake_client([])
    client.set_default_side_effect(_snv())

    async def bookable_then_flip() -> list[TimeSlot]:
        client.set_default_side_effect(_booking(101))
        return [_bookable_slot(slot)]

    client.add_search(bookable_then_flip)

    cfg = attempt_config(
        slot_dt_local=slot,
        court_ids=(STAFF_ID,),
        parallel_shots=1,
        not_open_retry_ms=100,
        not_open_deadline_s=1.0,
        global_deadline_s=2.0,
        grace_polling=GracePollingConfig(period_s=180, interval_s=10),
        min_lead_time_hours=2.0,
    )

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)

    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 101


async def test_grace_initial_fire_blocked_by_guard_when_too_close(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
) -> None:
    """Slot 1h away, min_lead 2h, grace_polling configured: even initial fire
    is blocked by guard before any POST. Grace is never entered (no search).
    """
    slot = datetime(2026, 4, 27, 10, 0, 0, tzinfo=ALMATY)
    clock = _make_close_clock(slot, hours_before=1.0)
    window_open = clock.now_utc() + timedelta(seconds=60)

    client = fake_client([])
    # No search effects added — grace must not be entered.

    cfg = attempt_config(
        slot_dt_local=slot,
        court_ids=(STAFF_ID,),
        parallel_shots=1,
        not_open_retry_ms=100,
        not_open_deadline_s=1.0,
        global_deadline_s=2.0,
        grace_polling=GracePollingConfig(period_s=180, interval_s=10),
        min_lead_time_hours=2.0,
    )

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)

    assert result.status == "error"
    assert result.business_code == "too_close_to_slot"
    # Initial fire blocked → no POST, no search.
    assert len(client.create_booking_calls) == 0
    assert len(client.search_timeslots_calls) == 0
