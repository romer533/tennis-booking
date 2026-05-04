from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import SecretStr

from tennis_booking.altegio import (
    AltegioConfig,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.altegio.client import AltegioClient
from tennis_booking.common.clock import Clock
from tennis_booking.common.tz import ALMATY
from tennis_booking.engine.attempt import AttemptConfig

BEARER = "test-bearer-secret"
COMPANY_ID = 521176
BOOKFORM_ID = 551098
BASE_URL = "https://b551098.alteg.io"
SERVICE_ID = 7849893
STAFF_ID = 1521566
SLOT = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)


class FakeClock:
    """Detached virtual clock: `now_utc` and `monotonic` advance only when `advance()` called.

    `sleep(seconds)` does NOT actually block — it records the request, yields control
    once (so other tasks can run), and auto-advances by the requested seconds.
    This lets the state-machine logic run through sleeps without real elapsed time.
    """

    def __init__(
        self,
        initial_utc: datetime,
        initial_mono: float = 1000.0,
    ) -> None:
        self._utc = initial_utc
        self._mono = initial_mono
        self._wall_drift = timedelta(0)
        self.sleep_calls: list[float] = []

    def now_utc(self) -> datetime:
        return self._utc + self._wall_drift

    def monotonic(self) -> float:
        return self._mono

    async def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        # Yield to scheduler so concurrent tasks (e.g. FakeAltegioClient awaitables) run.
        await asyncio.sleep(0)
        if seconds > 0:
            self.advance(seconds)
        await asyncio.sleep(0)

    def advance(self, seconds: float) -> None:
        self._mono += seconds
        self._utc += timedelta(seconds=seconds)

    def set_wall_drift(self, delta: timedelta) -> None:
        self._wall_drift = delta


SideEffect = BookingResponse | BaseException | Callable[[], Awaitable[BookingResponse]]
SearchEffect = (
    list[TimeSlot]
    | BaseException
    | Callable[[], Awaitable[list[TimeSlot]]]
)


class FakeAltegioClient:
    """FIFO script of create_booking responses. One call = one entry.

    Дополнительно поддерживает search_timeslots — отдельный FIFO список
    эффектов для grace mode тестов.
    """

    def __init__(
        self,
        side_effects: list[SideEffect] | None = None,
        config: AltegioConfig | None = None,
        prearm_effect: BaseException | Callable[[], Awaitable[None]] | None = None,
        search_effects: list[SearchEffect] | None = None,
        cancel_effects: list[BaseException | None] | None = None,
    ) -> None:
        self._side_effects: list[SideEffect] = list(side_effects or [])
        self._config = config or _default_config()
        self._prearm_effect = prearm_effect
        self._search_effects: list[SearchEffect] = list(search_effects or [])
        self._cancel_effects: list[BaseException | None] = list(cancel_effects or [])
        self._default_side_effect: SideEffect | None = None
        self.create_booking_calls: list[dict[str, Any]] = []
        self.prearm_calls: int = 0
        self.search_timeslots_calls: list[dict[str, Any]] = []
        self.cancel_booking_calls: list[dict[str, Any]] = []

    @property
    def config(self) -> AltegioConfig:
        return self._config

    def add(self, *effects: SideEffect) -> None:
        self._side_effects.extend(effects)

    def extend(self, effects: list[SideEffect]) -> None:
        self._side_effects.extend(effects)

    async def prearm(self) -> None:
        self.prearm_calls += 1
        if self._prearm_effect is None:
            await asyncio.sleep(0)
            return
        if isinstance(self._prearm_effect, BaseException):
            raise self._prearm_effect
        await self._prearm_effect()

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
        call = {
            "service_id": service_id,
            "staff_id": staff_id,
            "slot_dt_local": slot_dt_local,
            "fullname": fullname,
            "phone": phone,
            "email": email,
            "timeout_s": timeout_s,
        }
        self.create_booking_calls.append(call)
        if not self._side_effects:
            if self._default_side_effect is not None:
                effect: SideEffect = self._default_side_effect
            else:
                raise AssertionError("FakeAltegioClient has no more scripted responses")
        else:
            effect = self._side_effects.pop(0)
        if isinstance(effect, BookingResponse):
            await asyncio.sleep(0)
            return effect
        if isinstance(effect, BaseException):
            await asyncio.sleep(0)
            raise effect
        return await effect()

    def set_default_side_effect(self, effect: SideEffect | None) -> None:
        """Sticky default — used after explicit script is exhausted. Set None to disable."""
        self._default_side_effect = effect

    def add_search(self, *effects: SearchEffect) -> None:
        self._search_effects.extend(effects)

    def add_cancel(self, *effects: BaseException | None) -> None:
        self._cancel_effects.extend(effects)

    async def cancel_booking(
        self,
        record_id: int,
        record_hash: str,
        *,
        timeout_s: float | None = None,
    ) -> None:
        call = {
            "record_id": record_id,
            "record_hash": record_hash,
            "timeout_s": timeout_s,
        }
        self.cancel_booking_calls.append(call)
        if not self._cancel_effects:
            await asyncio.sleep(0)
            return
        effect = self._cancel_effects.pop(0)
        await asyncio.sleep(0)
        if effect is None:
            return
        raise effect

    async def search_timeslots(
        self,
        *,
        date_local: Any,
        staff_ids: list[int],
        timeout_s: float | None = None,
    ) -> list[TimeSlot]:
        call = {
            "date_local": date_local,
            "staff_ids": staff_ids,
            "timeout_s": timeout_s,
        }
        self.search_timeslots_calls.append(call)
        if not self._search_effects:
            raise AssertionError(
                "FakeAltegioClient has no more scripted search_timeslots responses"
            )
        effect = self._search_effects.pop(0)
        if isinstance(effect, list):
            await asyncio.sleep(0)
            return effect
        if isinstance(effect, BaseException):
            await asyncio.sleep(0)
            raise effect
        return await effect()


def _default_config(*, dry_run: bool = False) -> AltegioConfig:
    return AltegioConfig(
        bearer_token=SecretStr(BEARER),
        base_url=BASE_URL,
        company_id=COMPANY_ID,
        bookform_id=BOOKFORM_ID,
        dry_run=dry_run,
    )


@pytest.fixture
def fake_config() -> Callable[..., AltegioConfig]:
    def _make(**kwargs: Any) -> AltegioConfig:
        return _default_config(**kwargs)

    return _make


@pytest.fixture
def make_clock() -> Callable[..., FakeClock]:
    def _make(
        initial_utc: datetime | None = None, initial_mono: float = 1000.0
    ) -> FakeClock:
        return FakeClock(
            initial_utc=initial_utc or datetime(2026, 4, 23, 2, 0, 0, tzinfo=UTC),
            initial_mono=initial_mono,
        )

    return _make


@pytest.fixture
def fake_client() -> Callable[..., FakeAltegioClient]:
    def _make(
        side_effects: list[SideEffect] | None = None,
        *,
        dry_run: bool = False,
        prearm_effect: BaseException | Callable[[], Awaitable[None]] | None = None,
        search_effects: list[SearchEffect] | None = None,
        cancel_effects: list[BaseException | None] | None = None,
    ) -> FakeAltegioClient:
        return FakeAltegioClient(
            side_effects=side_effects,
            config=_default_config(dry_run=dry_run),
            prearm_effect=prearm_effect,
            search_effects=search_effects,
            cancel_effects=cancel_effects,
        )

    return _make


@pytest.fixture
def attempt_config() -> Callable[..., AttemptConfig]:
    def _make(**overrides: Any) -> AttemptConfig:
        # `court_id=X` shortcut for legacy single-court tests; expanded to court_ids=(X,).
        if "court_id" in overrides:
            cid = overrides.pop("court_id")
            overrides.setdefault("court_ids", (cid,))
        defaults: dict[str, Any] = {
            "slot_dt_local": SLOT,
            "court_ids": (STAFF_ID,),
            "service_id": SERVICE_ID,
            "fullname": "Roman",
            "phone": "77026473809",
            "profile_name": "roman",
            "email": None,
            "parallel_shots": 2,
            "not_open_retry_ms": 100,
            "not_open_deadline_s": 5.0,
            "global_deadline_s": 10.0,
            "prearm_lead_s": 30.0,
        }
        defaults.update(overrides)
        return AttemptConfig(**defaults)

    return _make


@pytest.fixture
def window_open() -> datetime:
    """Default window_open: 1 minute after FakeClock initial UTC (2026-04-23 02:00 UTC)."""
    return datetime(2026, 4, 23, 2, 1, 0, tzinfo=UTC)


def _assert_fake_client(value: object) -> FakeAltegioClient:
    assert isinstance(value, FakeAltegioClient)
    return value


# FakeAltegioClient is structurally compatible with AltegioClient interface used by engine
# (only create_booking + prearm + config are called). mypy is fine because we pass it as
# AltegioClient via runtime duck typing in tests.
def as_altegio_client(fake: FakeAltegioClient) -> AltegioClient:
    return fake  # type: ignore[return-value]


def as_clock(clock: FakeClock) -> Clock:
    return clock  # Protocol duck-type, mypy accepts at structural use sites


__all__ = [
    "BASE_URL",
    "BEARER",
    "BOOKFORM_ID",
    "COMPANY_ID",
    "FakeAltegioClient",
    "FakeClock",
    "SERVICE_ID",
    "SLOT",
    "STAFF_ID",
    "SearchEffect",
    "SideEffect",
    "as_altegio_client",
    "as_clock",
    "attempt_config",
    "fake_client",
    "fake_config",
    "make_clock",
    "window_open",
]
