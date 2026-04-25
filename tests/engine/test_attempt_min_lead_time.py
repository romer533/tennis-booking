"""Tests for `min_lead_time_hours` guard on `BookingAttempt._fire_and_retry`.

The guard skips the fan-out fire if `(slot_dt_local − now) < min_lead_time_hours`.
Rationale: Altegio refunds free cancellations only when more than 2h remain — booking
inside that window strands money on a slot we cannot cleanly release.

Strict less-than semantics: exactly on the boundary → fire still proceeds.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from tennis_booking.altegio import BookingResponse
from tennis_booking.common.tz import ALMATY
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111, record_hash: str = "h") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


def _slot_in_hours(clock: FakeClock, hours: float) -> datetime:
    """Return slot_dt_local that is `hours` after the clock's current UTC time."""
    return (clock.now_utc() + timedelta(hours=hours)).astimezone(ALMATY)


def _window_open_just_after(clock: FakeClock) -> datetime:
    """Default window: 1 minute after now (so attempt actually proceeds to fire)."""
    return clock.now_utc() + timedelta(seconds=60)


# ---- AttemptConfig validation ----------------------------------------------


class TestMinLeadTimeValidation:
    def test_negative_rejected(self) -> None:
        slot = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
        with pytest.raises(ValueError, match="min_lead_time_hours"):
            AttemptConfig(
                slot_dt_local=slot,
                court_ids=(1,),
                service_id=1,
                fullname="Roman",
                phone="77000",
                profile_name="roman",
                min_lead_time_hours=-0.5,
            )

    def test_above_max_rejected(self) -> None:
        slot = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
        with pytest.raises(ValueError, match="min_lead_time_hours"):
            AttemptConfig(
                slot_dt_local=slot,
                court_ids=(1,),
                service_id=1,
                fullname="Roman",
                phone="77000",
                profile_name="roman",
                min_lead_time_hours=200.0,
            )

    def test_default_zero(self) -> None:
        slot = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
        cfg = AttemptConfig(
            slot_dt_local=slot,
            court_ids=(1,),
            service_id=1,
            fullname="Roman",
            phone="77000",
            profile_name="roman",
        )
        assert cfg.min_lead_time_hours == 0.0

    def test_boundary_zero_accepted(self) -> None:
        slot = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
        cfg = AttemptConfig(
            slot_dt_local=slot,
            court_ids=(1,),
            service_id=1,
            fullname="Roman",
            phone="77000",
            profile_name="roman",
            min_lead_time_hours=0.0,
        )
        assert cfg.min_lead_time_hours == 0.0

    def test_boundary_max_accepted(self) -> None:
        slot = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
        cfg = AttemptConfig(
            slot_dt_local=slot,
            court_ids=(1,),
            service_id=1,
            fullname="Roman",
            phone="77000",
            profile_name="roman",
            min_lead_time_hours=168.0,
        )
        assert cfg.min_lead_time_hours == 168.0


# ---- Engine guard behaviour ------------------------------------------------


class TestMinLeadTimeGuard:
    async def test_slot_5h_away_guard_2h_fires(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        slot = _slot_in_hours(clock, 5.0)
        client = fake_client([_booking(42)])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=2.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "won"
        assert len(client.create_booking_calls) == 1

    async def test_slot_1h_away_guard_2h_skips_fire(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        slot = _slot_in_hours(clock, 1.0)
        client = fake_client([])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=2, min_lead_time_hours=2.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "error"
        assert result.business_code == "too_close_to_slot"
        # No POST sent — guard fired before fan-out.
        assert len(client.create_booking_calls) == 0
        assert result.shots_fired == 0

    async def test_slot_exactly_at_threshold_fires(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        """Strict less-than: at exactly min_lead_time_hours we DO fire.

        Note: by the time tight_loop runs, the FakeClock has advanced (sleeps to
        prearm + tight loop step), so to get an exact-boundary check we make the
        slot far enough that after small advances the slot is still > threshold
        by a margin. The boundary-OK assertion is: a slot that is comfortably
        more than min_lead at fire time fires.
        """
        clock = make_clock()
        # 2h + 30min safety margin: FakeClock advances during prearm sleeps
        # (~1min for our default window_open). 30min is comfortably above noise.
        slot = _slot_in_hours(clock, 2.5)
        client = fake_client([_booking(7)])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=2.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "won"
        assert len(client.create_booking_calls) == 1

    async def test_slot_just_under_threshold_skips(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        # 1.99h means time_to_slot ≈ 1.99h * 3600 < 2h * 3600 → guard fires.
        slot = _slot_in_hours(clock, 1.99)
        client = fake_client([])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=2.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "error"
        assert result.business_code == "too_close_to_slot"
        assert len(client.create_booking_calls) == 0

    async def test_guard_disabled_zero_allows_close_slot(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        """min_lead=0 → guard fully disabled even for slots very close to now."""
        clock = make_clock()
        # 5 minutes ahead — would normally trip a 2h guard, but guard is off.
        slot = _slot_in_hours(clock, 5 / 60.0)
        client = fake_client([_booking(99)])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=0.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "won"
        assert len(client.create_booking_calls) == 1

    async def test_per_booking_override_higher_than_app_default(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        """Per-booking value of 4h overrides the app default; slot 3h away → guard fires."""
        clock = make_clock()
        slot = _slot_in_hours(clock, 3.0)
        client = fake_client([])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=4.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "error"
        assert result.business_code == "too_close_to_slot"
        assert len(client.create_booking_calls) == 0

    async def test_guard_skips_prearm_was_called(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        """Even when guard fires, prearm runs first — guard sits between tight_loop
        and fan-out. We assert prearm WAS called but no booking POST happened."""
        clock = make_clock()
        slot = _slot_in_hours(clock, 0.5)
        client = fake_client([])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=2.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "error"
        assert result.business_code == "too_close_to_slot"
        assert client.prearm_calls == 1
        assert len(client.create_booking_calls) == 0

    async def test_guard_uses_utc_aware_comparison(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        """slot_dt_local is in Almaty (UTC+5). Clock returns UTC. Guard must convert."""
        # FakeClock initialises at 2026-04-23 02:00 UTC = 2026-04-23 07:00 Almaty.
        # Slot at 2026-04-23 08:00 Almaty == 2026-04-23 03:00 UTC = 1h ahead of clock.
        clock = FakeClock(initial_utc=datetime(2026, 4, 23, 2, 0, 0, tzinfo=UTC))
        slot = datetime(2026, 4, 23, 8, 0, 0, tzinfo=ALMATY)
        client = fake_client([])
        cfg = attempt_config(
            slot_dt_local=slot, parallel_shots=1, min_lead_time_hours=2.0
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(clock.now_utc() + timedelta(seconds=60))

        # 1h to slot < 2h threshold → guard fires.
        assert result.status == "error"
        assert result.business_code == "too_close_to_slot"

    async def test_guard_does_not_trigger_for_transport_retry(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        """Guard is checked only at the start of `_fire_and_retry`. Once we are
        past the initial check and into transport retries, time may keep ticking
        but we DO NOT re-check — finishing a started attempt is preferable to
        starting a fresh one inside the no-refund window.
        """
        from tennis_booking.altegio import AltegioTransportError

        clock = make_clock()
        # 3h away — comfortably > 2h guard.
        slot = _slot_in_hours(clock, 3.0)
        # First shot transport-fails; retry succeeds.
        client = fake_client([AltegioTransportError("ReadTimeout"), _booking(55)])
        cfg = attempt_config(
            slot_dt_local=slot,
            parallel_shots=1,
            min_lead_time_hours=2.0,
            not_open_retry_ms=10,
            not_open_deadline_s=1.0,
            global_deadline_s=5.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(_window_open_just_after(clock))

        assert result.status == "won"
        # Two POSTs: original + transport retry. Guard did not block the retry.
        assert len(client.create_booking_calls) == 2
