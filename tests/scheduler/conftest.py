from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from tennis_booking.altegio.client import AltegioClient
from tennis_booking.common.clock import Clock
from tennis_booking.engine.attempt import AttemptConfig, AttemptResult, BookingAttempt
from tennis_booking.persistence import BookingStore
from tennis_booking.scheduler.clock import CheckResult
from tennis_booking.scheduler.clock_errors import ClockDriftError, NTPUnreachableError

# Reuse FakeClock + FakeAltegioClient from engine tests — same shape, no need to duplicate.
from tests.engine.conftest import (  # noqa: F401 — re-exported as fixtures via star pattern below
    BASE_URL,
    BEARER,
    BOOKFORM_ID,
    COMPANY_ID,
    SERVICE_ID,
    SLOT,
    STAFF_ID,
    FakeAltegioClient,
    FakeClock,
    SideEffect,
    as_altegio_client,
    as_clock,
    attempt_config,
    fake_client,
    fake_config,
    make_clock,
    window_open,
)

NTPCheckerEffect = CheckResult | BaseException


class FakeNTPChecker:
    """Programmable NTP checker. Each await consumes one scripted effect (FIFO).

    If `loop=True` and the script is exhausted, the last effect is reused indefinitely.
    """

    def __init__(
        self,
        effects: list[NTPCheckerEffect] | None = None,
        loop: bool = False,
    ) -> None:
        self._effects: list[NTPCheckerEffect] = list(effects or [])
        self._loop = loop
        self.calls: int = 0
        self._last: NTPCheckerEffect | None = None

    async def __call__(self) -> CheckResult:
        self.calls += 1
        await asyncio.sleep(0)
        if not self._effects:
            if self._loop and self._last is not None:
                effect = self._last
            else:
                raise AssertionError("FakeNTPChecker has no more scripted effects")
        else:
            effect = self._effects.pop(0)
            self._last = effect
        if isinstance(effect, BaseException):
            raise effect
        return effect


def _ok_check(drift_ms: float = 5.0) -> CheckResult:
    return CheckResult(
        server="fake.ntp",
        ntp_time=datetime(2026, 4, 24, 0, 0, 0, tzinfo=UTC),
        drift_ms=drift_ms,
        rtt_ms=2.0,
    )


@pytest.fixture
def ok_ntp_check() -> Callable[..., CheckResult]:
    def _make(drift_ms: float = 5.0) -> CheckResult:
        return _ok_check(drift_ms)

    return _make


@pytest.fixture
def fake_ntp_checker() -> Callable[..., FakeNTPChecker]:
    def _make(
        effects: list[NTPCheckerEffect] | None = None,
        loop: bool = False,
    ) -> FakeNTPChecker:
        return FakeNTPChecker(effects=effects, loop=loop)

    return _make


@pytest.fixture
def ok_ntp_checker(fake_ntp_checker: Callable[..., FakeNTPChecker]) -> FakeNTPChecker:
    return fake_ntp_checker(effects=[_ok_check()], loop=True)


class FakeBookingAttempt:
    """Programmable BookingAttempt. Records run() calls; returns a scripted result
    after a scripted delay (using the injected Clock so tests stay deterministic).
    """

    def __init__(
        self,
        config: AttemptConfig,
        client: AltegioClient,
        clock: Clock,
        *,
        result: AttemptResult | None = None,
        delay_s: float = 0.0,
        raise_exc: BaseException | None = None,
    ) -> None:
        self.config = config
        self.client = client
        self.clock = clock
        self._result = result
        self._delay_s = delay_s
        self._raise_exc = raise_exc
        self.run_calls: list[datetime] = []

    async def run(self, window_open_utc: datetime) -> AttemptResult:
        self.run_calls.append(window_open_utc)
        if self._delay_s > 0:
            await self.clock.sleep(self._delay_s)
        if self._raise_exc is not None:
            raise self._raise_exc
        if self._result is None:
            return AttemptResult(
                status="won",
                booking=None,
                duplicates=(),
                fired_at_utc=window_open_utc,
                response_at_utc=window_open_utc,
                duration_ms=1.0,
                business_code=None,
                transport_cause=None,
                prearm_ok=True,
                shots_fired=1,
                attempt_id="fake-attempt",
            )
        return self._result


@pytest.fixture
def fake_attempt_factory() -> Callable[..., Any]:
    """Returns a function that builds an `attempt_factory` callable matching the loop's
    AttemptFactory signature, plus a list to inspect created instances.
    """

    def _make(
        result: AttemptResult | None = None,
        delay_s: float = 0.0,
        raise_exc: BaseException | None = None,
        results_per_call: list[AttemptResult] | None = None,
    ) -> tuple[Any, list[FakeBookingAttempt]]:
        created: list[FakeBookingAttempt] = []
        results_iter = iter(results_per_call) if results_per_call is not None else None

        def _factory(
            config: AttemptConfig,
            client: AltegioClient,
            clock: Clock,
            store: BookingStore | None = None,
        ) -> BookingAttempt:
            chosen_result: AttemptResult | None = result
            if results_iter is not None:
                try:
                    chosen_result = next(results_iter)
                except StopIteration:
                    pass
            instance = FakeBookingAttempt(
                config,
                client,
                clock,
                result=chosen_result,
                delay_s=delay_s,
                raise_exc=raise_exc,
            )
            created.append(instance)
            return instance  # type: ignore[return-value] — duck-typed BookingAttempt

        return _factory, created

    return _make


__all__ = [
    "BASE_URL",
    "BEARER",
    "BOOKFORM_ID",
    "COMPANY_ID",
    "SERVICE_ID",
    "SLOT",
    "STAFF_ID",
    "ClockDriftError",
    "FakeAltegioClient",
    "FakeBookingAttempt",
    "FakeClock",
    "FakeNTPChecker",
    "NTPUnreachableError",
    "SideEffect",
    "as_altegio_client",
    "as_clock",
    "attempt_config",
    "fake_attempt_factory",
    "fake_client",
    "fake_config",
    "fake_ntp_checker",
    "make_clock",
    "ok_ntp_check",
    "ok_ntp_checker",
    "window_open",
]
