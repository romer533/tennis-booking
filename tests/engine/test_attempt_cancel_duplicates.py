"""Tests for auto-cancel of duplicate bookings on multi-success fan-out.

Covers BookingAttempt (window phase) and PollAttempt (poll/post-window phase).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioTransportError,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt
from tennis_booking.engine.poll import PollAttempt, PollConfigData

from .conftest import (
    SERVICE_ID,
    SLOT,
    STAFF_ID,
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111, record_hash: str = "hash-a") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


# ---- BookingAttempt (window phase) -----------------------------------------


class TestWindowAttemptCancelDuplicates:
    async def test_window_attempt_cancels_duplicates_on_multi_success(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """3 shots, 3 succeed → 1 winner persisted, 2 cancelled."""
        clock = make_clock()
        client = fake_client(
            [_booking(1, "h1"), _booking(2, "h2"), _booking(3, "h3")],
            cancel_effects=[None, None],
        )
        # Multi-court pool (3 courts, 3 shots).
        cfg = attempt_config(court_ids=(1521566, 1521567, 1521568))

        attempt = BookingAttempt(
            cfg, as_altegio_client(client), as_clock(clock),
            cancel_duplicates_enabled=True,
        )
        result = await attempt.run(window_open)

        assert result.status == "won"
        won_id = result.booking.record_id  # type: ignore[union-attr]
        assert won_id in (1, 2, 3)

        # Total successes = 3; winners = 1; duplicates = 2 → cancel called twice.
        assert len(result.duplicates) == 2
        assert len(client.cancel_booking_calls) == 2

        # The two cancel calls target the dup record_ids, NOT the winner.
        cancelled_ids = {c["record_id"] for c in client.cancel_booking_calls}
        assert won_id not in cancelled_ids
        assert cancelled_ids == {1, 2, 3} - {won_id}

        # Each call carried a non-empty record_hash.
        for call in client.cancel_booking_calls:
            assert isinstance(call["record_hash"], str)
            assert call["record_hash"]

    async def test_window_attempt_cancel_failure_logged_attempt_still_wins(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Cancel HTTP fails → attempt still returns 'won', warning logged."""
        clock = make_clock()
        client = fake_client(
            [_booking(1, "h1"), _booking(2, "h2")],
            # Cancel fails with a transport error — must NOT propagate.
            cancel_effects=[AltegioTransportError("ReadTimeout")],
        )
        cfg = attempt_config(court_ids=(1521566, 1521567))

        attempt = BookingAttempt(
            cfg, as_altegio_client(client), as_clock(clock),
            cancel_duplicates_enabled=True,
        )
        result = await attempt.run(window_open)

        # Win is preserved despite cancel failing.
        assert result.status == "won"
        assert result.booking is not None
        assert len(client.cancel_booking_calls) == 1

    async def test_window_attempt_cancel_business_error_swallowed(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Business error from cancel (e.g. already cancelled) → win preserved."""
        clock = make_clock()
        client = fake_client(
            [_booking(1, "h1"), _booking(2, "h2")],
            cancel_effects=[
                AltegioBusinessError(
                    code="already_cancelled", message="x", http_status=422
                )
            ],
        )
        cfg = attempt_config(court_ids=(1521566, 1521567))

        attempt = BookingAttempt(
            cfg, as_altegio_client(client), as_clock(clock),
            cancel_duplicates_enabled=True,
        )
        result = await attempt.run(window_open)
        assert result.status == "won"
        assert len(client.cancel_booking_calls) == 1

    async def test_window_attempt_cancel_unknown_exception_swallowed(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Generic exception from cancel → win preserved (defensive)."""
        clock = make_clock()
        client = fake_client(
            [_booking(1, "h1"), _booking(2, "h2")],
            cancel_effects=[RuntimeError("boom")],
        )
        cfg = attempt_config(court_ids=(1521566, 1521567))

        attempt = BookingAttempt(
            cfg, as_altegio_client(client), as_clock(clock),
            cancel_duplicates_enabled=True,
        )
        result = await attempt.run(window_open)
        assert result.status == "won"
        assert len(client.cancel_booking_calls) == 1

    async def test_cancel_duplicates_disabled_via_kwarg_skip(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Flag False → no HTTP cancel calls; duplicates still tracked in result."""
        clock = make_clock()
        client = fake_client([_booking(1, "h1"), _booking(2, "h2")])
        cfg = attempt_config(court_ids=(1521566, 1521567))

        attempt = BookingAttempt(
            cfg, as_altegio_client(client), as_clock(clock),
            cancel_duplicates_enabled=False,
        )
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert len(result.duplicates) == 1
        # No cancel HTTP calls — feature off.
        assert client.cancel_booking_calls == []

    async def test_no_duplicates_no_cancel_calls(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Single shot, single success → no duplicates, no cancel."""
        clock = make_clock()
        client = fake_client([_booking(42, "h42")])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(
            cfg, as_altegio_client(client), as_clock(clock),
            cancel_duplicates_enabled=True,
        )
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.duplicates == ()
        assert client.cancel_booking_calls == []


# ---- PollAttempt (poll path) -----------------------------------------------


def _make_poll_attempt_config(**overrides: Any) -> AttemptConfig:
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


def _start_poll_clock() -> FakeClock:
    initial = (SLOT - timedelta(days=2)).astimezone(UTC)
    return FakeClock(initial_utc=initial, initial_mono=1000.0)


def _slot(dt_local: datetime, *, is_bookable: bool, staff_id: int | None = None) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=is_bookable, staff_id=staff_id)


SearchEffect = list[TimeSlot] | BaseException | Callable[[], Awaitable[list[TimeSlot]]]


class _PollFakeClient:
    """Minimal fake client supporting search_timeslots, create_booking, cancel_booking."""

    def __init__(
        self,
        *,
        search_effects: list[SearchEffect],
        booking_effects: list[BookingResponse | BaseException],
        cancel_effects: list[BaseException | None] | None = None,
    ) -> None:
        from pydantic import SecretStr

        from tennis_booking.altegio import AltegioConfig

        from .conftest import BASE_URL, BEARER, BOOKFORM_ID, COMPANY_ID

        self._search = list(search_effects)
        self._booking = list(booking_effects)
        self._cancel: list[BaseException | None] = list(cancel_effects or [])
        self._config = AltegioConfig(
            bearer_token=SecretStr(BEARER),
            base_url=BASE_URL,
            company_id=COMPANY_ID,
            bookform_id=BOOKFORM_ID,
        )
        self.search_calls: list[dict[str, Any]] = []
        self.booking_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[dict[str, Any]] = []

    @property
    def config(self) -> Any:
        return self._config

    async def search_timeslots(
        self, *, date_local: Any, staff_ids: list[int], timeout_s: float | None = None
    ) -> list[TimeSlot]:
        self.search_calls.append({"date_local": date_local, "staff_ids": staff_ids})
        await asyncio.sleep(0)
        eff = self._search.pop(0)
        if isinstance(eff, BaseException):
            raise eff
        if callable(eff):
            return await eff()
        return eff

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
        self.booking_calls.append({"staff_id": staff_id})
        await asyncio.sleep(0)
        eff = self._booking.pop(0)
        if isinstance(eff, BookingResponse):
            return eff
        raise eff

    async def cancel_booking(
        self,
        record_id: int,
        record_hash: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        self.cancel_calls.append(
            {
                "record_id": record_id,
                "record_hash": record_hash,
                "timeout_s": timeout_s,
            }
        )
        await asyncio.sleep(0)
        if not self._cancel:
            return
        effect = self._cancel.pop(0)
        if effect is None:
            return
        raise effect


def _as_client(fake: _PollFakeClient) -> Any:
    return fake


class TestPollAttemptCancelDuplicates:
    async def test_poll_attempt_cancels_duplicates(self) -> None:
        clock = _start_poll_clock()
        cid_a, cid_b = 1521566, 1521567
        fake = _PollFakeClient(
            search_effects=[
                # Both courts bookable → fan-out fires on both.
                [_slot(SLOT, is_bookable=True, staff_id=cid_a),
                 _slot(SLOT, is_bookable=True, staff_id=cid_b)],
            ],
            booking_effects=[_booking(101, "h101"), _booking(102, "h102")],
            cancel_effects=[None],  # one dup, one cancel call expected
        )
        cfg = _make_poll_attempt_config(court_ids=(cid_a, cid_b))
        poll = PollAttempt(
            cfg,
            PollConfigData(interval_s=10, start_offset_days=2),
            _as_client(fake),
            clock,
            cancel_duplicates_enabled=True,
        )
        result = await poll.run()
        assert result.status == "won"
        assert len(result.duplicates) == 1
        assert len(fake.cancel_calls) == 1
        # Cancelled record is the duplicate, NOT the winner.
        cancelled_id = fake.cancel_calls[0]["record_id"]
        assert cancelled_id != result.booking.record_id  # type: ignore[union-attr]
        assert cancelled_id in (101, 102)

    async def test_poll_attempt_cancel_failure_still_wins(self) -> None:
        clock = _start_poll_clock()
        cid_a, cid_b = 1521566, 1521567
        fake = _PollFakeClient(
            search_effects=[
                [_slot(SLOT, is_bookable=True, staff_id=cid_a),
                 _slot(SLOT, is_bookable=True, staff_id=cid_b)],
            ],
            booking_effects=[_booking(101, "h101"), _booking(102, "h102")],
            cancel_effects=[AltegioTransportError("server error 503")],
        )
        cfg = _make_poll_attempt_config(court_ids=(cid_a, cid_b))
        poll = PollAttempt(
            cfg,
            PollConfigData(interval_s=10, start_offset_days=2),
            _as_client(fake),
            clock,
            cancel_duplicates_enabled=True,
        )
        result = await poll.run()
        assert result.status == "won"
        # Cancel attempted once (and failed).
        assert len(fake.cancel_calls) == 1

    async def test_poll_attempt_cancel_disabled_no_calls(self) -> None:
        clock = _start_poll_clock()
        cid_a, cid_b = 1521566, 1521567
        fake = _PollFakeClient(
            search_effects=[
                [_slot(SLOT, is_bookable=True, staff_id=cid_a),
                 _slot(SLOT, is_bookable=True, staff_id=cid_b)],
            ],
            booking_effects=[_booking(101, "h101"), _booking(102, "h102")],
        )
        cfg = _make_poll_attempt_config(court_ids=(cid_a, cid_b))
        poll = PollAttempt(
            cfg,
            PollConfigData(interval_s=10, start_offset_days=2),
            _as_client(fake),
            clock,
            cancel_duplicates_enabled=False,
        )
        result = await poll.run()
        assert result.status == "won"
        assert len(result.duplicates) == 1
        assert fake.cancel_calls == []

    async def test_post_window_poll_cancels_duplicates(self) -> None:
        """post_window_mode path also cancels duplicates."""
        # Init clock such that we're in the window for post-window poll
        # (slot still in future, not too close).
        initial = (SLOT - timedelta(hours=12)).astimezone(UTC)
        clock = FakeClock(initial_utc=initial, initial_mono=1000.0)

        cid_a, cid_b = 1521566, 1521567
        fake = _PollFakeClient(
            search_effects=[
                [_slot(SLOT, is_bookable=True, staff_id=cid_a),
                 _slot(SLOT, is_bookable=True, staff_id=cid_b)],
            ],
            booking_effects=[_booking(201, "h201"), _booking(202, "h202")],
            cancel_effects=[None],
        )
        cfg = _make_poll_attempt_config(court_ids=(cid_a, cid_b))
        poll = PollAttempt(
            cfg,
            PollConfigData(interval_s=30, start_offset_days=1),
            _as_client(fake),
            clock,
            post_window_mode=True,
            cancel_duplicates_enabled=True,
        )
        result = await poll.run()
        assert result.status == "won"
        assert len(fake.cancel_calls) == 1
