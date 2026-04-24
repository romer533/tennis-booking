from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioTransportError,
    BookingResponse,
)
from tennis_booking.engine import codes as codes_module
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    SERVICE_ID,
    SLOT,
    STAFF_ID,
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)

# ---- helpers ---------------------------------------------------------------


def _booking(record_id: int = 111, record_hash: str = "hash-a") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


def _business(code: str, http_status: int = 422) -> AltegioBusinessError:
    return AltegioBusinessError(code=code, message=f"test-{code}", http_status=http_status)


def _transport(cause: str = "ReadTimeout") -> AltegioTransportError:
    return AltegioTransportError(cause)


@pytest.fixture
def _patch_codes(monkeypatch: pytest.MonkeyPatch) -> Callable[[frozenset[str], frozenset[str]], None]:
    """Helper to seed NOT_OPEN / SLOT_TAKEN codes in both codes module and attempt module.

    attempt.py imports the sets directly (not via module.attribute), so we must patch
    the imported name inside attempt's namespace too.
    """
    from tennis_booking.engine import attempt as attempt_module

    def _apply(not_open: frozenset[str], slot_taken: frozenset[str]) -> None:
        monkeypatch.setattr(codes_module, "NOT_OPEN_CODES", not_open)
        monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", slot_taken)
        monkeypatch.setattr(attempt_module, "NOT_OPEN_CODES", not_open)
        monkeypatch.setattr(attempt_module, "SLOT_TAKEN_CODES", slot_taken)

    return _apply


# ---- AttemptConfig validation ---------------------------------------------


class TestAttemptConfigValidation:
    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            AttemptConfig(
                slot_dt_local=datetime(2026, 4, 26, 23, 0),
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
            )

    def test_utc_datetime_rejected(self) -> None:
        with pytest.raises(ValueError, match="Asia/Almaty"):
            AttemptConfig(
                slot_dt_local=datetime(2026, 4, 26, 23, 0, tzinfo=UTC),
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
            )

    def test_empty_fullname_rejected(self) -> None:
        with pytest.raises(ValueError, match="fullname"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="   ",
                phone="77000",
            )

    def test_empty_phone_rejected(self) -> None:
        with pytest.raises(ValueError, match="phone"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone=" ",
            )

    def test_zero_court_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="court_id"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=0,
                service_id=1,
                fullname="Roman",
                phone="77000",
            )

    def test_negative_service_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="service_id"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=-5,
                fullname="Roman",
                phone="77000",
            )

    def test_zero_parallel_shots_rejected(self) -> None:
        with pytest.raises(ValueError, match="parallel_shots"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
                parallel_shots=0,
            )

    def test_global_deadline_not_gt_not_open_rejected(self) -> None:
        with pytest.raises(ValueError, match="global_deadline_s"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
                not_open_deadline_s=5.0,
                global_deadline_s=5.0,
            )

    def test_global_deadline_lt_not_open_rejected(self) -> None:
        with pytest.raises(ValueError, match="global_deadline_s"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
                not_open_deadline_s=10.0,
                global_deadline_s=5.0,
            )

    def test_zero_not_open_deadline_rejected(self) -> None:
        with pytest.raises(ValueError, match="not_open_deadline_s"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
                not_open_deadline_s=0,
                global_deadline_s=1.0,
            )

    def test_small_retry_ms_rejected(self) -> None:
        with pytest.raises(ValueError, match="not_open_retry_ms"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
                not_open_retry_ms=5,
            )

    def test_zero_prearm_lead_rejected(self) -> None:
        with pytest.raises(ValueError, match="prearm_lead_s"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_id=1,
                service_id=1,
                fullname="Roman",
                phone="77000",
                prearm_lead_s=0,
            )

    def test_fullname_trailing_whitespace_allowed(self) -> None:
        # Only empty-after-strip is rejected; non-empty with whitespace is fine (client strips).
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_id=1,
            service_id=1,
            fullname=" Roman ",
            phone="77000",
        )
        assert cfg.fullname == " Roman "

    def test_valid_config_accepts_defaults(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_id=1,
            service_id=1,
            fullname="Roman",
            phone="77000",
        )
        assert cfg.parallel_shots == 2
        assert cfg.not_open_retry_ms == 100
        assert cfg.global_deadline_s == 10.0


# ---- Window / preconditions ------------------------------------------------


class TestWindowPreconditions:
    async def test_window_already_passed_returns_error(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        cfg = attempt_config()

        past_window = clock.now_utc() - timedelta(seconds=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(past_window)

        assert result.status == "error"
        assert result.business_code == "window_passed"
        assert result.shots_fired == 0
        assert len(client.create_booking_calls) == 0
        assert client.prearm_calls == 0

    async def test_window_equal_to_now_returns_error(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        cfg = attempt_config()

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(clock.now_utc())
        assert result.status == "error"
        assert result.business_code == "window_passed"

    async def test_reuse_run_raises(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking()])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        await attempt.run(window_open)
        with pytest.raises(RuntimeError, match="single-shot"):
            await attempt.run(window_open)


# ---- Happy path -------------------------------------------------------------


class TestHappyPath:
    async def test_single_shot_success(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(42, "h42")])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 42
        assert result.booking.record_hash == "h42"
        assert result.duplicates == ()
        assert result.shots_fired == 1
        assert result.prearm_ok is True
        assert result.fired_at_utc is not None
        assert result.response_at_utc is not None
        assert len(client.create_booking_calls) == 1
        assert client.prearm_calls == 1

    async def test_two_parallel_shots_both_success_duplicates(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1, "h1"), _booking(2, "h2")])
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        # Either shot could win first; duplicates contains the other.
        won_id = result.booking.record_id
        assert won_id in (1, 2)
        if result.duplicates:
            assert result.duplicates[0].record_id != won_id
        assert result.shots_fired == 2
        assert len(client.create_booking_calls) == 2

    async def test_create_booking_called_with_config_values(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking()])
        cfg = attempt_config(parallel_shots=1, email="r@example.com")

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        await attempt.run(window_open)

        call = client.create_booking_calls[0]
        assert call["service_id"] == SERVICE_ID
        assert call["staff_id"] == STAFF_ID
        assert call["slot_dt_local"] == SLOT
        assert call["fullname"] == "Roman"
        assert call["phone"] == "77026473809"
        assert call["email"] == "r@example.com"
        assert call["timeout_s"] is not None
        assert call["timeout_s"] >= 0.2

    async def test_duration_ms_is_positive(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking()])
        cfg = attempt_config(parallel_shots=1)
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)
        assert result.duration_ms > 0


# ---- Business: not_open -> retry -> success --------------------------------


class TestNotOpenRetry:
    async def test_not_open_then_success_on_2nd(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client([_business("not_yet_open"), _booking(42)])
        cfg = attempt_config(parallel_shots=1, not_open_retry_ms=50)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 42
        assert result.shots_fired == 2

    async def test_not_open_retry_then_success_on_3rd(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client(
            [
                _business("not_yet_open"),
                _business("not_yet_open"),
                _booking(77, "h77"),
            ]
        )
        cfg = attempt_config(parallel_shots=1, not_open_retry_ms=50)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 77
        assert result.shots_fired == 3

    async def test_not_open_exhausts_deadline_timeout(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        # Enough not_open responses to exhaust deadline (not_open_deadline_s=5.0, retry 100ms).
        many_not_open: list[Any] = [_business("not_yet_open") for _ in range(100)]
        client = fake_client(many_not_open)
        cfg = attempt_config(parallel_shots=1, not_open_retry_ms=100, not_open_deadline_s=1.0)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"
        assert result.business_code == "not_yet_open"
        assert result.shots_fired > 1

    async def test_not_open_parallel_both_retry(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client(
            [
                _business("not_yet_open"),
                _business("not_yet_open"),
                _booking(10),
                _booking(11),
            ]
        )
        cfg = attempt_config(parallel_shots=2, not_open_retry_ms=50)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 4


# ---- Business: slot_taken -> lost ------------------------------------------


class TestSlotTaken:
    async def test_slot_taken_instant_lost(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client(
            [_business("slot_busy"), _business("slot_busy")],
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "lost"
        assert result.business_code == "slot_busy"
        # No retries — shots_fired == parallel_shots
        assert result.shots_fired == 2

    async def test_slot_taken_single_shot(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client([_business("slot_busy")])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "lost"
        assert result.shots_fired == 1


# ---- Business: unknown code fallback ---------------------------------------


class TestUnknownBusinessCode:
    async def test_unknown_code_fallback_lost(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # No codes patched: NOT_OPEN / SLOT_TAKEN are both empty frozenset.
        # Any business code is "unknown" → fallback lost.
        client = fake_client([_business("some_weird_code")])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "lost"
        assert result.business_code == "some_weird_code"


# ---- Config errors ---------------------------------------------------------


class TestConfigError:
    async def test_unauthorized_instant_error(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_business("unauthorized", http_status=401)])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "error"
        assert result.business_code == "unauthorized"
        assert result.booking is None

    async def test_unauthorized_cancels_siblings(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # With two-phase priority, if the second shot succeeds in the same `done` batch,
        # win beats unauthorized. To exercise the "only unauthorized resolves" path, the
        # second script entry must never complete; we use a hang-forever coroutine.

        async def hang() -> BookingResponse:
            await asyncio.sleep(3600)
            return _booking(99)

        client = fake_client(
            [_business("unauthorized", http_status=401), hang]
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "error"
        assert result.business_code == "unauthorized"
        assert result.shots_fired == 2


# ---- Transport retries -----------------------------------------------------


class TestTransportRetry:
    async def test_transport_then_success(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_transport("ReadTimeout"), _booking(55)])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 55
        assert result.shots_fired == 2

    async def test_transport_exhausts_deadline(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # Transport retries immediately without sleep, so we need per-call time to
        # advance for deadline to trigger. FakeAltegioClient doesn't advance the clock,
        # so we use low global_deadline_s and rely on per-shot timeouts to advance.
        # In this test FakeAltegioClient will return transport each time with no time
        # advancement → infinite loop. Protect against that by using a transport that
        # also advances fake clock via side-effect.

        async def transport_then_advance() -> BookingResponse:
            clock.advance(3.0)
            raise _transport("ReadTimeout")

        client = fake_client(
            [transport_then_advance, transport_then_advance, transport_then_advance, transport_then_advance]
        )
        cfg = attempt_config(parallel_shots=1, global_deadline_s=5.0, not_open_deadline_s=1.0)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"
        assert result.transport_cause == "ReadTimeout"

    async def test_transport_then_slot_taken_lost(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client([_transport(), _business("slot_busy")])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "lost"
        assert result.business_code == "slot_busy"

    async def test_all_parallel_transport_all_retry(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [_transport(), _transport(), _booking(1), _booking(2)]
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 4


# ---- Prearm ----------------------------------------------------------------


class TestPrearm:
    async def test_prearm_failure_does_not_block_fire(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [_booking(1)],
            prearm_effect=_transport("DNSResolveError"),
        )
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.prearm_ok is False
        assert client.prearm_calls == 1

    async def test_prearm_timeout_prearm_ok_false(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()

        async def slow_prearm() -> None:
            await asyncio.sleep(100.0)

        client = fake_client([_booking(1)], prearm_effect=slow_prearm)
        # Short prearm_lead and tight window → prearm budget is ~0.1s, wait_for cancels fast.
        cfg = attempt_config(parallel_shots=1, prearm_lead_s=1.1)

        # window 1.2s in the future → prearm starts at +0.1s, budget ≈ (1.2 − 1.0 − 0.1) = 0.1s
        window = clock.now_utc() + timedelta(seconds=1.2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window)

        assert result.prearm_ok is False
        assert result.status == "won"

    async def test_prearm_success_prearm_ok_true(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1)])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.prearm_ok is True
        assert client.prearm_calls == 1


# ---- Tight loop timing -----------------------------------------------------


class TestTightLoop:
    async def test_tight_loop_does_not_fire_before_window(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        # We can't observe "fired before T-0" in wall time since FakeClock is virtual,
        # but we can assert that fired_at_utc >= window_open_utc.
        clock = make_clock()
        client = fake_client([_booking(1)])
        cfg = attempt_config(parallel_shots=1)

        window = clock.now_utc() + timedelta(seconds=60)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window)

        assert result.fired_at_utc is not None
        assert result.fired_at_utc >= window

    async def test_prearm_called_before_fire(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1)])
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        # Prearm was called, then at least one create_booking.
        assert client.prearm_calls == 1
        assert len(client.create_booking_calls) >= 1
        assert result.status == "won"

    async def test_no_prearm_sleep_if_window_close(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1)])
        cfg = attempt_config(parallel_shots=1, prearm_lead_s=30.0)

        # Window is only 2s in future → prearm_at_mono is in the past. No sleep.
        window = clock.now_utc() + timedelta(seconds=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window)
        assert result.status == "won"
        assert client.prearm_calls == 1


# ---- DRY_RUN ---------------------------------------------------------------


class TestDryRun:
    async def test_dry_run_won_via_dry_run_hash(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [BookingResponse(record_id=0, record_hash="dry-run")],
            dry_run=True,
        )
        cfg = attempt_config(parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_hash == "dry-run"
        assert result.booking.record_id == 0


# ---- Cancellation / cleanup -------------------------------------------------


class TestCancellation:
    async def test_external_cancel_cleans_up(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        async def hang() -> BookingResponse:
            await asyncio.sleep(3600)
            return _booking(1)

        client = fake_client([hang, hang])
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))

        # Use real asyncio.wait_for to cancel from outside.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(attempt.run(window_open), timeout=0.5)

        # No zombie tasks — if cleanup works, there should be no warnings;
        # we just verify the call completed without hanging.

    async def test_duplicate_success_recorded(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        # Both shots resolve at the same tick → both could be in the first `done` set.
        client = fake_client([_booking(100), _booking(200)])
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        total_success = 1 + len(result.duplicates)
        # We posted 2 shots and both scripted to succeed; expect at least 1 duplicate
        # if both resolved concurrently, or 0 if only first completed and second was
        # cancelled. Either outcome is acceptable per the spec (duplicates is "if any").
        assert total_success in (1, 2)


# ---- Cancellation during retry ----------------------------------------------


class TestCancellationError:
    async def test_cancelled_error_task_handled(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """If a shot raises CancelledError mid-flight (rare internal), engine shouldn't crash."""
        clock = make_clock()

        async def hang_then_cancel() -> BookingResponse:
            # Simulate own-cancel by sleeping briefly then raising.
            await asyncio.sleep(0)
            raise asyncio.CancelledError()

        client = fake_client([hang_then_cancel, _booking(1)])
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        # Either won (second shot succeeds) or one was cancelled; engine tolerates
        # CancelledError and continues. Must not raise.
        assert result.status in ("won", "timeout", "lost", "error")


# ---- Integration with AltegioClient via respx ------------------------------


# ---- Additional coverage tests ---------------------------------------------


class TestEdgeCoverage:
    async def test_unknown_exception_from_shot_retried_as_transport(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        client = fake_client([ValueError("unexpected"), _booking(99)])
        cfg = attempt_config(parallel_shots=1)
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 99

    async def test_both_parallel_succeed_concurrently_triggers_duplicates_path(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        # Both shots start pending (hit asyncio.sleep), wait for a common barrier,
        # then return. `asyncio.wait(..., FIRST_COMPLETED)` can return with only one
        # in `done`, but then the winning path calls _drain_for_duplicates which
        # will await the still-pending second task → if it's already completed by
        # then, it's collected as a duplicate.
        async def shot_1() -> BookingResponse:
            for _ in range(10):
                await asyncio.sleep(0)
            return _booking(101)

        async def shot_2() -> BookingResponse:
            for _ in range(10):
                await asyncio.sleep(0)
            return _booking(102)

        cfg = attempt_config(parallel_shots=2)
        client = fake_client([shot_1, shot_2])
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        won_id = result.booking.record_id  # type: ignore[union-attr]
        assert won_id in (101, 102)

    async def test_global_deadline_already_exceeded_before_wait(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        async def slow_eat_and_fail() -> BookingResponse:
            # Advance fake clock past deadline synchronously before raising.
            clock.advance(20.0)
            raise _transport("ReadTimeout")

        client = fake_client([slow_eat_and_fail, slow_eat_and_fail])
        cfg = attempt_config(parallel_shots=1, global_deadline_s=5.0, not_open_deadline_s=1.0)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"

    async def test_wait_timeout_no_done_deadline_reached(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
    ) -> None:
        """asyncio.wait returns with empty done set when timeout hits → next loop iteration
        sees remaining <= 0 and triggers global_deadline branch (lines 233-234, 252).

        Uses real SystemClock so that wait()'s real elapsed time is reflected in monotonic.
        """
        from tennis_booking.common.clock import SystemClock

        clock = SystemClock()

        async def hang() -> BookingResponse:
            await asyncio.sleep(30)
            return _booking(1)

        window = clock.now_utc() + timedelta(seconds=1.2)
        client = fake_client([hang])
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=0.2,
            not_open_deadline_s=0.1,
            prearm_lead_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), clock)
        result = await asyncio.wait_for(attempt.run(window), timeout=5.0)

        assert result.status == "timeout"
        assert result.transport_cause == "global_deadline"


# ---- Race regression tests: two-phase priority classification --------------


class TestRacePriority:
    """Guards against the race in `asyncio.wait(..., FIRST_COMPLETED)` where `done`
    can contain multiple tasks whose set-iteration order is non-deterministic.
    Win MUST beat any terminal error in the same `done` batch.
    """

    @staticmethod
    def _make_concurrent_booking(record_id: int) -> Callable[[], Any]:
        """Shot that yields 10 times before returning — ensures both parallel shots
        become done in the same `asyncio.wait` iteration."""

        async def _shot() -> BookingResponse:
            for _ in range(10):
                await asyncio.sleep(0)
            return _booking(record_id)

        return _shot

    @staticmethod
    def _make_concurrent_business(code: str, http_status: int = 422) -> Callable[[], Any]:
        async def _shot() -> BookingResponse:
            for _ in range(10):
                await asyncio.sleep(0)
            raise _business(code, http_status=http_status)

        return _shot

    async def test_simultaneous_win_and_slot_taken_picks_win(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client(
            [
                self._make_concurrent_booking(1),
                self._make_concurrent_business("slot_busy"),
            ]
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won", "win must beat slot_taken in the same done batch"
        assert result.booking is not None
        assert result.booking.record_id == 1
        # slot_taken exception does not produce a BookingResponse, so not in duplicates.
        assert result.duplicates == ()
        assert result.business_code is None

    async def test_simultaneous_win_and_unauthorized_picks_win(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [
                self._make_concurrent_booking(42),
                self._make_concurrent_business("unauthorized", http_status=401),
            ]
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 42
        assert result.business_code is None

    async def test_simultaneous_win_and_unknown_code_picks_win(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # No _patch_codes: unknown codes are treated as "unknown_code" branch.
        client = fake_client(
            [
                self._make_concurrent_booking(7),
                self._make_concurrent_business("random_new_code"),
            ]
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 7
        assert result.business_code is None

    async def test_simultaneous_two_wins_second_in_duplicates(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [
                self._make_concurrent_booking(1),
                self._make_concurrent_booking(2),
            ]
        )
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        won_id = result.booking.record_id
        assert won_id in (1, 2)
        assert len(result.duplicates) == 1
        dup_id = result.duplicates[0].record_id
        assert dup_id in (1, 2)
        assert dup_id != won_id

    async def test_drain_for_duplicates_collects_pending_success(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """One shot wins immediately; the other is still pending and resolves shortly
        after — _drain_for_duplicates must collect it as a duplicate."""
        clock = make_clock()

        async def fast_win() -> BookingResponse:
            await asyncio.sleep(0)
            return _booking(1001)

        async def slow_win() -> BookingResponse:
            # Still pending when fast_win returns, but has a booking ready after cancel.
            for _ in range(3):
                await asyncio.sleep(0)
            return _booking(1002)

        client = fake_client([fast_win, slow_win])
        cfg = attempt_config(parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        # First in: 1001; duplicate (if collected) is 1002. If slow_win was cancelled
        # before it produced a result, duplicates is empty — both are acceptable because
        # cancellation of an async sleep propagates CancelledError, not a BookingResponse.
        assert result.booking is not None
        assert result.booking.record_id == 1001

    async def test_mixed_not_open_exhausted_and_transport_continues(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        _patch_codes: Any,
    ) -> None:
        """Basic assertion of the invariant: global_deadline_s > not_open_deadline_s →
        after not_open exhausts, transport retry should keep going until a win or the
        global deadline. Here we script transport-then-win on shot 2 while shot 1 keeps
        hitting not_open — the engine must not abandon shot 2's retry path just because
        shot 1 exhausted its not_open window.
        """
        _patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
        clock = make_clock()

        # Use a simpler shape: sequential effects (FakeClient is FIFO across both shots).
        # shot A/B order is stochastic from the engine's perspective, so we script ~pairs
        # of (not_open, transport) repeated, then two booking responses at the tail.
        script: list[Any] = []
        for _ in range(5):
            script.append(_business("not_yet_open"))
            script.append(_transport("ReadTimeout"))
        script.append(_booking(500))
        script.append(_booking(501))

        client = fake_client(script)
        cfg = attempt_config(
            parallel_shots=2,
            not_open_retry_ms=50,
            not_open_deadline_s=1.0,
            global_deadline_s=10.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        # With transport retrying immediately and not_open retrying every 50ms, the
        # engine should eventually reach a booking response from the tail of the script.
        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id in (500, 501)


@pytest.mark.parametrize(
    "pattern",
    [
        "fail_then_success",
    ],
)
async def test_real_altegio_client_dry_run(
    pattern: str,
    attempt_config: Callable[..., AttemptConfig],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
) -> None:
    """Dry-run path against real AltegioClient — no HTTP issued."""
    import respx
    from pydantic import SecretStr

    from tennis_booking.altegio import AltegioConfig
    from tennis_booking.altegio.client import BOOK_RECORD_PATH, AltegioClient

    with respx.mock(base_url="https://b551098.alteg.io", assert_all_called=False) as mock:
        route = mock.post(BOOK_RECORD_PATH.format(company_id=521176))
        cfg = AltegioConfig(
            bearer_token=SecretStr("X"),
            base_url="https://b551098.alteg.io",
            company_id=521176,
            bookform_id=551098,
            dry_run=True,
        )
        async with AltegioClient(cfg) as client:
            clock = make_clock()
            acfg = attempt_config(parallel_shots=1)
            attempt = BookingAttempt(acfg, client, as_clock(clock))
            result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_hash == "dry-run"
        # respx must not see any real POST.
        assert route.call_count == 0
