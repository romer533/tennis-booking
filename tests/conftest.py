from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import pytest

from tennis_booking.scheduler import clock as clock_module


class FakeNTPClient:
    """In-memory NTPClient for unit tests.

    Each call to `fetch` consumes one scripted response from `responses` (FIFO).
    An entry is either:
      - a tuple (ntp_time, rtt_ms) to return,
      - an Exception (or subclass) instance to raise.
    """

    def __init__(self, responses: list[tuple[datetime, float] | BaseException]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[str, float]] = []

    async def fetch(self, server: str, timeout_s: float) -> tuple[datetime, float]:
        self.calls.append((server, timeout_s))
        if not self._responses:
            raise AssertionError("FakeNTPClient has no more scripted responses")
        item = self._responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    @property
    def call_count(self) -> int:
        return len(self.calls)


@pytest.fixture
def fake_ntp_factory() -> Callable[..., FakeNTPClient]:
    def _make(responses: list[tuple[datetime, float] | BaseException]) -> FakeNTPClient:
        return FakeNTPClient(responses)

    return _make


@pytest.fixture
def fake_ntp_client(fake_ntp_factory: Callable[..., FakeNTPClient]) -> FakeNTPClient:
    """Default fake: single response with NTP time == local now, 3ms RTT."""
    return fake_ntp_factory([(datetime.now(tz=UTC), 3.0)])


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[datetime], None]]:
    """Patch datetime.now inside scheduler.clock to a fixed value."""
    state: dict[str, datetime] = {"now": datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)}

    class _FrozenDatetime:
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            value = state["now"]
            if tz is None:
                return value.replace(tzinfo=None)
            return value.astimezone(tz)

    monkeypatch.setattr(clock_module, "datetime", _FrozenDatetime)

    def _set(new_now: datetime) -> None:
        state["now"] = new_now

    yield _set


__all__ = [
    "FakeNTPClient",
    "fake_ntp_client",
    "fake_ntp_factory",
    "frozen_now",
]
