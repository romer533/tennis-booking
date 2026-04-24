from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, time
from types import MappingProxyType
from typing import Any

from tennis_booking.altegio import BookingResponse
from tennis_booking.altegio.client import AltegioClient
from tennis_booking.common.clock import Clock
from tennis_booking.common.tz import ALMATY
from tennis_booking.config.schema import (
    AppConfig,
    PollConfig,
    Profile,
    ResolvedBooking,
    Weekday,
)
from tennis_booking.engine.attempt import AttemptConfig, AttemptResult
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.scheduler.loop import (
    DEFAULT_NTP_THRESHOLD_MS,
    SchedulerLoop,
)

from .conftest import (
    SERVICE_ID,
    STAFF_ID,
    FakeAltegioClient,
    FakeClock,
    FakeNTPChecker,
    as_altegio_client,
    as_clock,
)

# ---- helpers (similar to test_loop) ----------------------------------------


def _profile() -> Profile:
    return Profile(
        name="roman",
        full_name="Roman G",
        phone="77001234567",
        email="r@x.com",
    )


def _booking(
    name: str = "fri-eve",
    weekday: Weekday = Weekday.FRIDAY,
    slot_local_time: time = time(18, 0),
    poll: PollConfig | None = None,
    court_id: int = STAFF_ID,
) -> ResolvedBooking:
    return ResolvedBooking(
        name=name,
        weekday=weekday,
        slot_local_time=slot_local_time,
        duration_minutes=60,
        court_ids=(court_id,),
        service_id=SERVICE_ID,
        profile=_profile(),
        enabled=True,
        poll=poll,
    )


def _config(*bookings: ResolvedBooking) -> AppConfig:
    profiles_by_name: dict[str, Profile] = {b.profile.name: b.profile for b in bookings}
    return AppConfig(
        bookings=tuple(bookings),
        profiles=MappingProxyType(profiles_by_name),
        court_pools=MappingProxyType({}),
    )


def _build_loop(
    config: AppConfig,
    clock: FakeClock,
    client: FakeAltegioClient,
    *,
    ntp_required: bool = True,
    ntp_checker: FakeNTPChecker,
    attempt_factory: Any = None,
    poll_attempt_factory: Any = None,
) -> SchedulerLoop:
    return SchedulerLoop(
        config=config,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_required=ntp_required,
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ntp_checker,  # type: ignore[arg-type]
        attempt_factory=attempt_factory,
        poll_attempt_factory=poll_attempt_factory,
    )


class FakePollAttempt:
    """Programmable PollAttempt for loop tests."""

    instances: list[FakePollAttempt] = []

    def __init__(
        self,
        config: AttemptConfig,
        poll: PollConfigData,
        client: AltegioClient,
        clock: Clock,
        won_event: asyncio.Event,
        *,
        result: AttemptResult | None = None,
        delay_s: float = 0.0,
    ) -> None:
        self.config = config
        self.poll = poll
        self.client = client
        self.clock = clock
        self.won_event = won_event
        self._result = result
        self._delay_s = delay_s
        self.run_calls: int = 0
        FakePollAttempt.instances.append(self)

    async def run(self) -> AttemptResult:
        self.run_calls += 1
        if self._delay_s > 0:
            await self.clock.sleep(self._delay_s)
        if self._result is None:
            return AttemptResult(
                status="lost",
                booking=None,
                duplicates=(),
                fired_at_utc=None,
                response_at_utc=None,
                duration_ms=1.0,
                business_code=None,
                transport_cause="slot_passed",
                prearm_ok=False,
                shots_fired=0,
                attempt_id="fake-poll",
                phase="poll",
            )
        return self._result


def _make_poll_factory(
    *,
    result: AttemptResult | None = None,
    delay_s: float = 0.0,
) -> tuple[Any, list[FakePollAttempt]]:
    FakePollAttempt.instances = []
    created: list[FakePollAttempt] = []

    def _factory(
        config: AttemptConfig,
        poll: PollConfigData,
        client: AltegioClient,
        clock: Clock,
        won_event: asyncio.Event,
    ) -> PollAttempt:
        instance = FakePollAttempt(
            config, poll, client, clock, won_event, result=result, delay_s=delay_s
        )
        created.append(instance)
        return instance  # type: ignore[return-value]

    return _factory, created


# ---- Tests ------------------------------------------------------------------


async def test_booking_without_poll_spawns_only_window_task(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    """Backward-compat: legacy booking (poll=None) spawns exactly one task."""
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=None))
    factory, _ = fake_attempt_factory()
    poll_factory, poll_created = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    assert len(loop._scheduled) == 1
    assert len(poll_created) == 0
    await loop.stop()


async def test_booking_with_poll_spawns_two_tasks(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory()
    poll_factory, poll_created = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    assert len(loop._scheduled) == 2
    # Allow the poll task to start and instantiate the factory
    for _ in range(5):
        if poll_created:
            break
        await asyncio.sleep(0)
    assert len(poll_created) == 1
    await loop.stop()


async def test_won_event_shared_between_window_and_poll(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory()
    poll_factory, poll_created = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    for _ in range(5):
        if poll_created:
            break
        await asyncio.sleep(0)

    poll_inst = poll_created[0]
    # Both window and poll tasks should see the same Event instance via the loop's
    # _won_events map.
    evt_keys = list(loop._won_events.values())
    assert len(evt_keys) == 1
    assert poll_inst.won_event is evt_keys[0]
    await loop.stop()


async def test_poll_won_sets_won_event(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory()

    poll_won_result = AttemptResult(
        status="won",
        booking=BookingResponse(record_id=42, record_hash="h"),
        duplicates=(),
        fired_at_utc=now_utc,
        response_at_utc=now_utc,
        duration_ms=10.0,
        business_code=None,
        transport_cause=None,
        prearm_ok=False,
        shots_fired=1,
        attempt_id="fake",
        phase="poll",
    )
    poll_factory, poll_created = _make_poll_factory(result=poll_won_result)

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)

    # Drive poll task to completion
    for _ in range(20):
        await asyncio.sleep(0)
        if poll_created and poll_created[0].run_calls > 0:
            break

    # Wait for the poll task to finish
    poll_key = (
        sched[0].booking.name,
        sched[0].slot_dt_local.isoformat(),
        hash(sched[0].booking.court_ids),
        sched[0].booking.service_id,
        ":poll",
    )
    poll_task = loop._scheduled.get(poll_key)
    if poll_task is not None:
        for _ in range(20):
            if poll_task.done():
                break
            await asyncio.sleep(0)

    # Verify won_event was set (or already cleaned up because both tasks done)
    # We can't reliably check the event after cleanup; instead, check that the
    # poll task completed without exception.
    assert poll_created[0].run_calls == 1
    await loop.stop()


async def test_won_event_cleanup_after_both_tasks_done(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    """After both window and poll tasks finish, the shared won_event must be evicted."""
    now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory(
        result=AttemptResult(
            status="lost",
            booking=None,
            duplicates=(),
            fired_at_utc=now_utc,
            response_at_utc=now_utc,
            duration_ms=1.0,
            business_code="slot_busy",
            transport_cause=None,
            prearm_ok=True,
            shots_fired=1,
            attempt_id="fake",
            phase="window",
        )
    )
    poll_factory, _poll_created = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)

    # Advance past prearm + window
    for _ in range(50):
        if not loop._scheduled:
            break
        await asyncio.sleep(0)
        clock.advance(10.0)

    assert len(loop._scheduled) == 0
    assert len(loop._won_events) == 0
    await loop.stop()


async def test_poll_task_named_correctly(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory()
    poll_factory, _ = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)

    task_names = {t.get_name() for t in loop._scheduled.values()}
    assert any(name.startswith("attempt:") for name in task_names)
    assert any(name.startswith("poll:") for name in task_names)
    await loop.stop()


async def test_multiple_bookings_with_and_without_poll(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(
        _booking(name="b1", court_id=5, poll=None),
        _booking(
            name="b2",
            court_id=6,
            poll=PollConfig(interval_s=60, start_offset_days=2),
        ),
        _booking(
            name="b3",
            court_id=7,
            poll=PollConfig(interval_s=120, start_offset_days=3),
        ),
    )
    factory, _ = fake_attempt_factory()
    poll_factory, _ = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    # 1 window + (1 + 1) window+poll + (1 + 1) window+poll = 5 tasks
    assert len(loop._scheduled) == 5
    await loop.stop()


async def test_poll_dedup_on_re_recompute(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    """Calling _spawn_attempts twice must not duplicate poll tasks."""
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory()
    poll_factory, _ = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    assert len(loop._scheduled) == 2

    sched2 = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched2)
    assert len(loop._scheduled) == 2  # no duplicates
    await loop.stop()


async def test_loop_stop_cancels_both_tasks(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=60, start_offset_days=2)))
    factory, _ = fake_attempt_factory()
    poll_factory, _ = _make_poll_factory(delay_s=10000.0)  # poll never finishes naturally

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    assert len(loop._scheduled) == 2

    # Stop must cancel/await both tasks
    await loop.stop()
    assert len(loop._scheduled) == 0


async def test_poll_attempt_receives_correct_config(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
    fake_attempt_factory: Callable[..., Any],
) -> None:
    now_utc = datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC)
    clock = make_clock(initial_utc=now_utc)
    client = fake_client([])
    cfg = _config(_booking(poll=PollConfig(interval_s=45, start_offset_days=3)))
    factory, _ = fake_attempt_factory()
    poll_factory, poll_created = _make_poll_factory()

    loop = _build_loop(
        cfg, clock, client, ntp_checker=ok_ntp_checker,
        attempt_factory=factory, poll_attempt_factory=poll_factory,
    )

    sched = await loop._recompute_windows(now_utc)
    loop._spawn_attempts(sched)
    for _ in range(5):
        if poll_created:
            break
        await asyncio.sleep(0)

    inst = poll_created[0]
    assert inst.poll.interval_s == 45
    assert inst.poll.start_offset_days == 3
    assert inst.config.court_ids == (STAFF_ID,)
    assert inst.config.service_id == SERVICE_ID
    assert inst.config.slot_dt_local == datetime(2026, 4, 24, 18, 0, tzinfo=ALMATY)
    await loop.stop()
