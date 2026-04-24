from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Protocol

__all__ = ["Clock", "SystemClock"]


class Clock(Protocol):
    """Абстракция над системными часами — делаем engine/loop тестируемым без реального сна."""

    def now_utc(self) -> datetime: ...

    def monotonic(self) -> float: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    """Production implementation. Отдельно от scheduler/clock.py — у того другой concern (NTP drift)."""

    def now_utc(self) -> datetime:
        return datetime.now(tz=UTC)

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            # asyncio.sleep(0) yields to loop exactly once; negative is a no-op by contract.
            await asyncio.sleep(0)
            return
        await asyncio.sleep(seconds)
