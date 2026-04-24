import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from tennis_booking.scheduler import (
    CheckResult,
    ClockDriftError,
    NTPResponseError,
    NTPUnreachableError,
    check_ntp_drift,
)
from tests.conftest import FakeNTPClient

FROZEN_NOW = datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _ok(delta_ms: float, rtt_ms: float = 5.0) -> tuple[datetime, float]:
    """Build an NTP reply whose time differs from FROZEN_NOW by delta_ms.

    Positive delta_ms: NTP is ahead → local is behind → drift_ms is negative.
    Negative delta_ms: NTP is behind → local is ahead → drift_ms is positive.
    """
    return (FROZEN_NOW + timedelta(milliseconds=delta_ms), rtt_ms)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
class TestHappyPath:
    async def test_drift_zero_ok(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(0.0)])
        result = await check_ntp_drift(client=fake)
        assert result.drift_ms == pytest.approx(0.0, abs=1e-6)

    async def test_drift_under_10ms_ok(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-8.0)])  # local 8ms ahead
        result = await check_ntp_drift(client=fake)
        assert result.drift_ms == pytest.approx(8.0, abs=1e-6)

    async def test_drift_just_under_threshold_ok(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-49.0)])
        result = await check_ntp_drift(client=fake)
        assert result.drift_ms == pytest.approx(49.0, abs=1e-6)

    async def test_custom_threshold_100ms_allows_80ms(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-80.0)])
        result = await check_ntp_drift(client=fake, threshold_ms=100)
        assert result.drift_ms == pytest.approx(80.0, abs=1e-6)

    async def test_result_fields(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        ntp_time = FROZEN_NOW + timedelta(milliseconds=-5.0)
        fake = fake_ntp_factory([(ntp_time, 7.3)])
        result = await check_ntp_drift(client=fake, server="example.ntp")

        assert isinstance(result, CheckResult)
        assert result.server == "example.ntp"
        assert result.ntp_time == ntp_time
        assert result.rtt_ms == pytest.approx(7.3)
        assert result.drift_ms == pytest.approx(5.0, abs=1e-6)

    async def test_result_ntp_time_is_utc_aware(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(0.0)])
        result = await check_ntp_drift(client=fake)
        assert result.ntp_time.tzinfo is not None
        assert result.ntp_time.utcoffset() == timedelta(0)


# ---------------------------------------------------------------------------
# Negative drift / threshold
# ---------------------------------------------------------------------------
class TestDriftExceeded:
    async def test_drift_just_over_threshold_raises(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-51.0)])  # local 51ms ahead → drift_ms = +51
        with pytest.raises(ClockDriftError) as exc_info:
            await check_ntp_drift(client=fake, server="t.example")
        exc = exc_info.value
        assert exc.drift_ms == pytest.approx(51.0, abs=1e-6)
        assert exc.threshold_ms == 50
        assert exc.server == "t.example"

    async def test_drift_large_positive_raises(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-500.0)])  # drift = +500 ms
        with pytest.raises(ClockDriftError) as exc_info:
            await check_ntp_drift(client=fake)
        assert "500" in str(exc_info.value)

    async def test_drift_negative_beyond_threshold_raises(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(51.0)])  # local 51ms behind → drift_ms = -51
        with pytest.raises(ClockDriftError) as exc_info:
            await check_ntp_drift(client=fake)
        assert exc_info.value.drift_ms == pytest.approx(-51.0, abs=1e-6)

    async def test_drift_negative_small_ok(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(10.0)])  # local 10ms behind
        result = await check_ntp_drift(client=fake)
        assert result.drift_ms == pytest.approx(-10.0, abs=1e-6)

    async def test_threshold_zero_any_drift_raises(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-1.0)])
        with pytest.raises(ClockDriftError):
            await check_ntp_drift(client=fake, threshold_ms=0)


# ---------------------------------------------------------------------------
# Network errors / retries
# ---------------------------------------------------------------------------
class TestNetworkErrors:
    async def test_ntp_unreachable_unknown_host(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        err = NTPUnreachableError("unknown.host", "DNS resolution failed: nodename nor servname provided")
        fake = fake_ntp_factory([err, err])
        with pytest.raises(NTPUnreachableError) as exc_info:
            await check_ntp_drift(client=fake, server="unknown.host", backoff_s=0)
        assert exc_info.value.server == "unknown.host"

    async def test_udp_timeout_raises_unreachable(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        fake = fake_ntp_factory(
            [
                NTPUnreachableError("t.example", "timeout after 2.0s"),
                NTPUnreachableError("t.example", "timeout after 2.0s"),
            ]
        )
        with pytest.raises(NTPUnreachableError) as exc_info:
            await check_ntp_drift(client=fake, server="t.example", backoff_s=0)
        # Critically: not asyncio.TimeoutError.
        assert not isinstance(exc_info.value, asyncio.TimeoutError)

    async def test_malformed_response_raises_ntp_response_error(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        err = NTPResponseError("t.example", "expected 48 bytes, got 12")
        fake = fake_ntp_factory([err, err])
        with pytest.raises(NTPResponseError):
            await check_ntp_drift(client=fake, server="t.example", backoff_s=0)

    def test_ntp_response_error_is_subclass_of_unreachable(self) -> None:
        assert issubclass(NTPResponseError, NTPUnreachableError)

    async def test_retry_success_on_second_attempt(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory(
            [NTPUnreachableError("t.example", "timeout"), _ok(0.0)]
        )
        result = await check_ntp_drift(client=fake, server="t.example", backoff_s=0)
        assert result.drift_ms == pytest.approx(0.0, abs=1e-6)
        assert fake.call_count == 2

    async def test_retry_both_fail(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        fake = fake_ntp_factory(
            [
                NTPUnreachableError("t.example", "timeout"),
                NTPUnreachableError("t.example", "timeout"),
            ]
        )
        with pytest.raises(NTPUnreachableError):
            await check_ntp_drift(client=fake, server="t.example", retries=1, backoff_s=0)
        assert fake.call_count == 2

    async def test_no_retries_single_attempt(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        fake = fake_ntp_factory([NTPUnreachableError("t.example", "timeout")])
        with pytest.raises(NTPUnreachableError):
            await check_ntp_drift(client=fake, server="t.example", retries=0, backoff_s=0)
        assert fake.call_count == 1

    async def test_ntp_error_semantic_stratum_zero(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        err = NTPResponseError("t.example", "invalid stratum 0")
        fake = fake_ntp_factory([err, err])
        with pytest.raises(NTPResponseError):
            await check_ntp_drift(client=fake, server="t.example", backoff_s=0)

    async def test_no_network_immediate_fail_fast(
        self, fake_ntp_factory: Callable[..., FakeNTPClient]
    ) -> None:
        fake = fake_ntp_factory([NTPUnreachableError("t.example", "OSError: Network unreachable")])
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        with pytest.raises(NTPUnreachableError):
            await check_ntp_drift(client=fake, server="t.example", retries=0, backoff_s=0)
        assert (loop.time() - t0) < 0.1


# ---------------------------------------------------------------------------
# Precondition validation (before any network)
# ---------------------------------------------------------------------------
class TestPreconditions:
    async def test_negative_threshold_raises_value_error(
        self, fake_ntp_client: FakeNTPClient
    ) -> None:
        with pytest.raises(ValueError, match="threshold_ms"):
            await check_ntp_drift(client=fake_ntp_client, threshold_ms=-1)
        assert fake_ntp_client.call_count == 0

    async def test_zero_timeout_raises_value_error(
        self, fake_ntp_client: FakeNTPClient
    ) -> None:
        with pytest.raises(ValueError, match="timeout_s"):
            await check_ntp_drift(client=fake_ntp_client, timeout_s=0)
        assert fake_ntp_client.call_count == 0

    async def test_negative_timeout_raises_value_error(
        self, fake_ntp_client: FakeNTPClient
    ) -> None:
        with pytest.raises(ValueError, match="timeout_s"):
            await check_ntp_drift(client=fake_ntp_client, timeout_s=-0.1)
        assert fake_ntp_client.call_count == 0

    async def test_negative_retries_raises_value_error(
        self, fake_ntp_client: FakeNTPClient
    ) -> None:
        with pytest.raises(ValueError, match="retries"):
            await check_ntp_drift(client=fake_ntp_client, retries=-1)
        assert fake_ntp_client.call_count == 0

    async def test_empty_server_raises_value_error(
        self, fake_ntp_client: FakeNTPClient
    ) -> None:
        with pytest.raises(ValueError, match="server"):
            await check_ntp_drift(client=fake_ntp_client, server="")
        assert fake_ntp_client.call_count == 0

    async def test_whitespace_server_raises_value_error(
        self, fake_ntp_client: FakeNTPClient
    ) -> None:
        with pytest.raises(ValueError, match="server"):
            await check_ntp_drift(client=fake_ntp_client, server="   ")
        assert fake_ntp_client.call_count == 0


# ---------------------------------------------------------------------------
# Semantic / edge
# ---------------------------------------------------------------------------
class TestSemantics:
    async def test_ntp_time_in_future_negative_drift(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        """NTP time > local now → local is behind → drift_ms < 0."""
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(20.0)])  # NTP is 20ms ahead of local
        result = await check_ntp_drift(client=fake)
        assert result.drift_ms < 0
        assert result.drift_ms == pytest.approx(-20.0, abs=1e-6)

    async def test_ntp_time_in_past_positive_drift(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        """NTP time < local now → local is ahead → drift_ms > 0."""
        frozen_now(FROZEN_NOW)
        fake = fake_ntp_factory([_ok(-20.0)])
        result = await check_ntp_drift(client=fake)
        assert result.drift_ms > 0
        assert result.drift_ms == pytest.approx(20.0, abs=1e-6)

    async def test_concurrent_calls_independent(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        fakes = [fake_ntp_factory([_ok(-i * 1.0)]) for i in range(3)]
        results = await asyncio.gather(
            *(check_ntp_drift(client=f) for f in fakes)
        )
        assert [r.drift_ms for r in results] == pytest.approx([0.0, 1.0, 2.0], abs=1e-6)

    async def test_second_call_after_first_failure(
        self,
        fake_ntp_factory: Callable[..., FakeNTPClient],
        frozen_now: Callable[[datetime], None],
    ) -> None:
        frozen_now(FROZEN_NOW)
        # First invocation: single attempt, fails.
        fake1 = fake_ntp_factory([NTPUnreachableError("t.example", "x")])
        with pytest.raises(NTPUnreachableError):
            await check_ntp_drift(client=fake1, retries=0, backoff_s=0)
        # Second invocation on fresh client: succeeds.
        fake2 = fake_ntp_factory([_ok(0.0)])
        result = await check_ntp_drift(client=fake2)
        assert result.drift_ms == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# Integration (skipped by default)
# ---------------------------------------------------------------------------
@pytest.mark.integration
async def test_real_ntp_cloudflare_smoke() -> None:
    result = await check_ntp_drift(
        server="time.cloudflare.com",
        threshold_ms=1000,
        timeout_s=3.0,
        retries=1,
    )
    assert result.server == "time.cloudflare.com"
    assert abs(result.drift_ms) < 1000
    assert 0 < result.rtt_ms < 5000
    assert result.ntp_time.utcoffset() == timedelta(0)
