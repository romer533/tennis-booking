"""Engine tests for exponential backoff on transport-retry path.

Defensive measure against Cloudflare per-IP rate-rule (~30 RPS): instead of
immediately re-firing a shot after `AltegioTransportError`, the engine waits
exponentially before each retry. Cloudflare cause uses a more aggressive schedule
(100ms → 200 → 400 → 800 → 1600 → cap 2000ms); other transport causes use a
lighter schedule (50 → 100 → 200 → cap 500ms).

Per-shot delay does NOT block sibling shots: each retry runs as its own task.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from tennis_booking.altegio import AltegioTransportError, BookingResponse
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111) -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=f"h{record_id}")


def _cloudflare() -> AltegioTransportError:
    return AltegioTransportError("cloudflare_challenge")


def _other_transport(cause: str = "ReadTimeout") -> AltegioTransportError:
    return AltegioTransportError(cause)


# ---------- helpers ----------


def _backoff_sleeps(clock: FakeClock) -> list[float]:
    """Sleeps recorded after the prearm/tight-loop phase. We strip:
      - the leading prearm-related sleep(s) and tight-loop micro-sleeps
        (these are sub-second and fixed; backoff sleeps are >= 0.05s).
    """
    # Backoff delays we expect: 0.05s..2.0s. Tight loop uses 0.001s steps.
    # Prearm/sleep_until uses larger sleeps too — but those happen BEFORE any
    # transport error. Strategy: filter on (>= 0.05s and <= 2.0s).
    return [s for s in clock.sleep_calls if 0.04 < s <= 2.05]


# ---------- CF backoff schedule ----------


class TestCloudflareExponentialBackoff:
    async def test_first_cf_retry_uses_100ms(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # 1 CF, then success. → exactly one backoff sleep of 0.1s.
        client = fake_client([_cloudflare(), _booking(1)])
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=10.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        assert delays == [0.1]

    async def test_second_cf_retry_uses_200ms(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_cloudflare(), _cloudflare(), _booking(1)])
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=10.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        assert delays == [0.1, 0.2]

    async def test_third_cf_retry_uses_400ms(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [_cloudflare(), _cloudflare(), _cloudflare(), _booking(1)]
        )
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=10.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        assert delays == [0.1, 0.2, 0.4]

    async def test_cf_retry_caps_at_2s(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """6 CF + success: delays 0.1, 0.2, 0.4, 0.8, 1.6, cap 2.0.
        Cumulative sleep ~5.1s, plus per-shot read timeouts. Use 30s deadline.
        """
        clock = make_clock()
        client = fake_client(
            [
                _cloudflare(),
                _cloudflare(),
                _cloudflare(),
                _cloudflare(),
                _cloudflare(),
                _cloudflare(),
                _booking(1),
            ]
        )
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=30.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        # 6 retries (one per CF) → 6 backoff sleeps.
        assert delays == [0.1, 0.2, 0.4, 0.8, 1.6, 2.0]

    async def test_cf_retry_caps_persists_at_2s(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """7+ CF: each subsequent retry stays at 2.0s (cap)."""
        clock = make_clock()
        client = fake_client(
            [
                _cloudflare(),  # 1: 0.1s
                _cloudflare(),  # 2: 0.2s
                _cloudflare(),  # 3: 0.4s
                _cloudflare(),  # 4: 0.8s
                _cloudflare(),  # 5: 1.6s
                _cloudflare(),  # 6: 2.0s (cap)
                _cloudflare(),  # 7: 2.0s (cap)
                _booking(1),
            ]
        )
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=60.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        assert delays == [0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0]


# ---------- Other transport backoff schedule ----------


class TestOtherTransportLowerBackoff:
    async def test_first_other_transport_retry_uses_50ms(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_other_transport("ReadTimeout"), _booking(1)])
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=10.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        assert delays == [0.05]

    async def test_other_transport_retry_caps_at_500ms(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """50 → 100 → 200 → 400 → cap 500."""
        clock = make_clock()
        client = fake_client(
            [
                _other_transport("ReadTimeout"),
                _other_transport("ReadTimeout"),
                _other_transport("ReadTimeout"),
                _other_transport("ReadTimeout"),
                _other_transport("ReadTimeout"),
                _booking(1),
            ]
        )
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=10.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        delays = _backoff_sleeps(clock)
        assert delays == [0.05, 0.1, 0.2, 0.4, 0.5]


# ---------- Deadline-aware skip ----------


class TestRetrySkippedNearDeadline:
    async def test_retry_skipped_if_close_to_global_deadline(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """When backoff_delay > remaining_to_deadline, skip the retry and
        let the attempt time out cleanly with transport_cause set.
        """
        clock = make_clock()

        async def cf_then_burn() -> BookingResponse:
            # Burn 4.7s of clock so global_deadline (5s) is nearly exhausted
            # by the time we classify and decide on backoff.
            clock.advance(4.7)
            raise _cloudflare()

        client = fake_client([cf_then_burn])
        # global=5s, not_open=1s → after the first response there is < 0.4s left,
        # well below CF backoff 0.1s + margin? Actually 0.1 + 0.1 margin = 0.2s
        # and we have ~0.3s left, so retry happens. Need to burn closer.

        # Better: burn 4.95s — then 0.05s remains, < 0.1+0.1=0.2s required.
        async def cf_then_burn_more() -> BookingResponse:
            clock.advance(4.95)
            raise _cloudflare()

        clock = make_clock()
        client = fake_client([cf_then_burn_more])
        cfg = attempt_config(
            parallel_shots=1,
            global_deadline_s=5.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        # No retry was scheduled — the attempt drains and returns timeout.
        assert result.status == "timeout"
        assert result.transport_cause == "cloudflare_challenge"
        # Only the original shot fired — no retry.
        assert result.shots_fired == 1
        # No backoff sleep was ever issued.
        delays = _backoff_sleeps(clock)
        assert delays == []


# ---------- Sibling shots not blocked ----------


class TestBackoffDoesNotBlockSiblings:
    async def test_one_shot_backoff_does_not_block_other_shot_success(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Two-court pool: shot 0 returns CF, shot 1 returns success.
        Shot 0's retry is queued with backoff but shot 1's success terminates
        the attempt before the backoff completes.
        """
        clock = make_clock()
        # Order matters: FakeAltegioClient is FIFO and shots are spawned
        # 0 then 1 in that order. Shot 0 → CF; shot 1 → win.
        client = fake_client([_cloudflare(), _booking(99)])
        cfg = attempt_config(
            court_ids=(101, 102),
            global_deadline_s=10.0,
            not_open_deadline_s=1.0,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 99


__all__: list[str] = []
