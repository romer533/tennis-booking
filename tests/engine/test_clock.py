from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime

from tennis_booking.common.clock import SystemClock


class TestSystemClock:
    def test_now_utc_is_timezone_aware(self) -> None:
        clock = SystemClock()
        now = clock.now_utc()
        assert isinstance(now, datetime)
        assert now.tzinfo is UTC

    def test_monotonic_is_non_decreasing(self) -> None:
        clock = SystemClock()
        a = clock.monotonic()
        time.sleep(0.05)  # Windows monotonic resolution is ~16ms.
        b = clock.monotonic()
        assert b >= a
        assert b - a >= 0.04

    async def test_sleep_zero_yields_once(self) -> None:
        clock = SystemClock()
        start = time.monotonic()
        await clock.sleep(0)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05  # sanity — should be effectively instant

    async def test_sleep_negative_no_block(self) -> None:
        clock = SystemClock()
        start = time.monotonic()
        await clock.sleep(-1.0)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05

    async def test_sleep_positive_blocks(self) -> None:
        clock = SystemClock()
        start = time.monotonic()
        await clock.sleep(0.05)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.04

    async def test_concurrent_sleeps(self) -> None:
        clock = SystemClock()
        start = time.monotonic()
        await asyncio.gather(clock.sleep(0.02), clock.sleep(0.02), clock.sleep(0.02))
        elapsed = time.monotonic() - start
        # Should be concurrent — ~0.02s, not 0.06s.
        assert elapsed < 0.05
