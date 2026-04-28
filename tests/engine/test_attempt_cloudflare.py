"""Engine behaviour when a fan-out shot returns a Cloudflare challenge.

Production observation 28.04 02:00 UTC: in a 553-response fire ~6% came
back as Cloudflare 403 + html. Pre-fix client classified those as
business `unknown` → engine fallback "lost" (no retry). Post-fix client
raises `AltegioTransportError(cause="cloudflare_challenge")` and engine
follows the existing transport-retry path until `global_deadline_s`.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from tennis_booking.altegio import AltegioBusinessError, AltegioTransportError, BookingResponse
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111, record_hash: str = "hash-a") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


def _snv() -> AltegioBusinessError:
    return AltegioBusinessError(
        code="service_not_available",
        message="The service is not available at the selected time",
        http_status=422,
    )


def _cloudflare() -> AltegioTransportError:
    return AltegioTransportError("cloudflare_challenge")


async def test_cloudflare_in_first_batch_retries_via_transport_path(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
) -> None:
    """Fan-out 7 shots, batch 1 = 1 cloudflare + 6 snv. Engine must NOT bail
    on `unknown_code_fallback` — cloudflare is transport, so it joins the
    retry pool with snv shots. Eventually a retry succeeds → won.
    """
    clock = make_clock()
    # First batch (7 shots): one cloudflare, six service_not_available.
    # Engine retries: snv shots ride the not_open path; cloudflare shot rides
    # the transport path. To keep the script bounded, success comes on retry.
    client = fake_client(
        [
            _cloudflare(),  # shot 0 → transport retry
            _snv(),  # shot 1
            _snv(),  # shot 2
            _snv(),  # shot 3
            _snv(),  # shot 4
            _snv(),  # shot 5
            _snv(),  # shot 6
        ]
    )
    # Subsequent retries: keep returning snv until somebody wins.
    client.set_default_side_effect(_booking(1))

    cfg = attempt_config(
        parallel_shots=7,
        not_open_retry_ms=100,
        not_open_deadline_s=2.0,
        global_deadline_s=5.0,
    )

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)

    # Critical: NOT a "lost code=unknown" fallback. Either won (retry succeeded)
    # or timeout (deadline) — but no unknown_code business path.
    assert result.business_code != "unknown"
    assert result.status in ("won", "timeout")
    # And we definitely fired more than the initial 7 shots — the cloudflare shot
    # was retried, not bailed-on.
    assert result.shots_fired > 7


async def test_cloudflare_only_retries_until_global_deadline(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
) -> None:
    """All shots return cloudflare → engine retries on transport path,
    eventually times out with transport_cause='cloudflare_challenge'.

    FakeClock doesn't auto-advance for transport-retry path (no sleep there),
    so each shot itself advances the clock — same pattern as the existing
    `test_all_parallel_transport_all_retry_until_deadline` test.
    """
    clock = make_clock()

    async def cloudflare_then_advance() -> BookingResponse:
        clock.advance(3.0)
        raise _cloudflare()

    client = fake_client(
        [cloudflare_then_advance, cloudflare_then_advance, cloudflare_then_advance, cloudflare_then_advance]
    )
    cfg = attempt_config(
        parallel_shots=1,
        not_open_deadline_s=1.0,
        global_deadline_s=5.0,
    )

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)

    assert result.status == "timeout"
    assert result.transport_cause == "cloudflare_challenge"
    assert result.business_code is None
