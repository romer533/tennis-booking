from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, time
from types import MappingProxyType
from typing import Any

import pytest

from tennis_booking.altegio import BookingResponse
from tennis_booking.common.tz import ALMATY
from tennis_booking.config.schema import (
    AppConfig,
    Profile,
    ResolvedBooking,
    Weekday,
)
from tennis_booking.engine.attempt import AttemptConfig, AttemptResult
from tennis_booking.scheduler.clock_errors import ClockDriftError, NTPUnreachableError
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

# ---------- helpers ----------------------------------------------------------


def _profile(name: str = "roman") -> Profile:
    return Profile(
        name=name,
        full_name=f"Roman {name}",
        phone="77001234567",
        email="r@example.com",
    )


def _booking(
    name: str = "fri-eve",
    weekday: Weekday = Weekday.FRIDAY,
    slot_local_time: time = time(18, 0),
    court_id: int = STAFF_ID,
    profile: Profile | None = None,
    enabled: bool = True,
    duration_minutes: int = 60,
) -> ResolvedBooking:
    return ResolvedBooking(
        name=name,
        weekday=weekday,
        slot_local_time=slot_local_time,
        duration_minutes=duration_minutes,
        court_id=court_id,
        profile=profile or _profile(),
        enabled=enabled,
    )


def _config(*bookings: ResolvedBooking) -> AppConfig:
    profiles_by_name: dict[str, Profile] = {}
    for b in bookings:
        profiles_by_name.setdefault(b.profile.name, b.profile)
    return AppConfig(
        bookings=tuple(bookings),
        profiles=MappingProxyType(profiles_by_name),
    )


def _build_loop(
    config: AppConfig,
    clock: FakeClock,
    client: FakeAltegioClient,
    *,
    ntp_required: bool = True,
    ntp_threshold_ms: int = DEFAULT_NTP_THRESHOLD_MS,
    ntp_checker: FakeNTPChecker | None = None,
    attempt_factory: Any = None,
    recompute_local_time: time = time(6, 55),
) -> SchedulerLoop:
    return SchedulerLoop(
        config=config,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_required=ntp_required,
        ntp_threshold_ms=ntp_threshold_ms,
        ntp_checker=ntp_checker,  # type: ignore[arg-type] — FakeNTPChecker is callable
        attempt_factory=attempt_factory,
        recompute_local_time=recompute_local_time,
    )


def _almaty_to_utc(local_dt: datetime) -> datetime:
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=ALMATY)
    return local_dt.astimezone(UTC)


# ---------- 1. NTP guard -----------------------------------------------------


class TestStartupNTP:
    async def test_start_normal_ntp_ok(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        factory, _ = fake_attempt_factory()
        cfg = _config()  # empty — loop has nothing to do, but startup must succeed

        loop = _build_loop(
            cfg, clock, client,
            ntp_checker=ok_ntp_checker,
            attempt_factory=factory,
        )

        # Run loop.run() and stop it shortly after — startup NTP check must pass.
        run_task = asyncio.create_task(loop.run())
        await asyncio.sleep(0)
        await loop.stop()
        await run_task
        assert ok_ntp_checker.calls >= 1

    async def test_start_clock_drift_required_raises(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        ntp = fake_ntp_checker(
            effects=[ClockDriftError(120.0, 50, "fake.ntp")], loop=False
        )
        factory, _ = fake_attempt_factory()
        cfg = _config(_booking())

        loop = _build_loop(
            cfg, clock, client, ntp_required=True, ntp_checker=ntp,
            attempt_factory=factory,
        )

        with pytest.raises(ClockDriftError):
            await loop.run()

    async def test_start_clock_drift_optional_warns(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        ntp = fake_ntp_checker(
            effects=[ClockDriftError(120.0, 50, "fake.ntp")], loop=True
        )
        factory, _ = fake_attempt_factory()
        cfg = _config()

        loop = _build_loop(
            cfg, clock, client,
            ntp_required=False, ntp_checker=ntp, attempt_factory=factory,
        )

        run_task = asyncio.create_task(loop.run())
        await asyncio.sleep(0)
        await loop.stop()
        await run_task

    async def test_start_ntp_unreachable_required_raises(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        ntp = fake_ntp_checker(
            effects=[NTPUnreachableError("fake.ntp", "timeout")], loop=False
        )
        factory, _ = fake_attempt_factory()
        cfg = _config()

        loop = _build_loop(
            cfg, clock, client, ntp_required=True, ntp_checker=ntp,
            attempt_factory=factory,
        )

        with pytest.raises(NTPUnreachableError):
            await loop.run()

    async def test_start_ntp_unreachable_optional_warns(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        ntp = fake_ntp_checker(
            effects=[NTPUnreachableError("fake.ntp", "timeout")], loop=True
        )
        factory, _ = fake_attempt_factory()
        cfg = _config()

        loop = _build_loop(
            cfg, clock, client,
            ntp_required=False, ntp_checker=ntp, attempt_factory=factory,
        )

        run_task = asyncio.create_task(loop.run())
        await asyncio.sleep(0)
        await loop.stop()
        await run_task


# ---------- 2. Recompute timing ---------------------------------------------


class TestRecomputeTiming:
    async def test_next_recompute_at_06_54_59_returns_06_55(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # 06:54:59 Almaty = 01:54:59 UTC
        clock = make_clock(initial_utc=datetime(2026, 4, 24, 1, 54, 59, tzinfo=UTC))
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        target = loop._next_recompute_at(clock.now_utc())
        target_local = target.astimezone(ALMATY)
        assert target_local.hour == 6 and target_local.minute == 55
        assert (target - clock.now_utc()).total_seconds() == pytest.approx(1.0, abs=0.01)

    async def test_next_recompute_at_06_55_00_returns_now(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 24, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        target = loop._next_recompute_at(clock.now_utc())
        # exact match → trigger now
        assert target == clock.now_utc()

    async def test_next_recompute_at_06_55_01_returns_tomorrow(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 24, 1, 55, 1, tzinfo=UTC))
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        target = loop._next_recompute_at(clock.now_utc())
        # missed by 1s → next is tomorrow 06:55
        delta = (target - clock.now_utc()).total_seconds()
        # 24h - 1s = 86399 — but loop will still recompute today on first iteration,
        # this method just returns the next *daily* recompute time
        assert 86398 < delta < 86401


# ---------- 3. Recompute logic ----------------------------------------------


class TestRecomputeLogic:
    async def test_empty_config_zero_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 24, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert scheduled == []

    async def test_all_disabled_zero_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 24, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(name="b1", enabled=False), _booking(name="b2", enabled=False))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert scheduled == []

    async def test_one_enabled_one_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # Tuesday 2026-04-21 06:55 Almaty = 01:55 UTC.
        # Friday 18:00 Almaty slot opens Tuesday 07:00 → in 5 minutes.
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(name="fri", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.booking.name == "fri"
        # Slot is Friday 2026-04-24 18:00 Almaty (this Friday)
        assert sa.slot_dt_local == datetime(2026, 4, 24, 18, 0, tzinfo=ALMATY)
        # Window opens Tuesday 2026-04-21 07:00 Almaty = 02:00 UTC
        assert sa.window_open_utc == datetime(2026, 4, 21, 2, 0, tzinfo=UTC)

    async def test_three_enabled_two_disabled_three_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(
            _booking(name="b1"),
            _booking(name="b2"),
            _booking(name="b3"),
            _booking(name="dis1", enabled=False),
            _booking(name="dis2", enabled=False),
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 3

    async def test_window_in_past_skipped_with_warn(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # 07:30 Almaty Tuesday = 02:30 UTC. Friday 18:00 slot's window opened at
        # Tuesday 07:00 → already 30 min ago → skip.
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 2, 30, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(name="fri", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert scheduled == []

    async def test_friday_18_slot_recompute_tuesday_06_55(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # Tuesday 06:55 Almaty = 01:55 UTC, slot Fri 18:00 → window in 5 min
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 1
        # Window opens 5 minutes from now
        assert (scheduled[0].window_open_utc - clock.now_utc()).total_seconds() == 300

    async def test_year_boundary_january_1_slot(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # 2026-01-01 was Thursday. Slot Thursday 12:00, "now" is Mon 2025-12-29 06:55
        # Almaty = Sun 2025-12-29 01:55 UTC. Window: T-3 = Mon 2025-12-29 07:00 Almaty.
        clock = make_clock(initial_utc=datetime(2025, 12, 29, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.THURSDAY, slot_local_time=time(12, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.slot_dt_local == datetime(2026, 1, 1, 12, 0, tzinfo=ALMATY)
        assert sa.window_open_utc == datetime(2025, 12, 29, 2, 0, tzinfo=UTC)

    async def test_two_identical_bookings_two_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(
            _booking(name="b1", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)),
            _booking(name="b2", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)),
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 2

    def test_next_slot_same_day_past_time_rolls_to_next_week(
        self,
    ) -> None:
        # Friday 20:00 Almaty; slot is Friday 18:00 (already passed today).
        # next occurrence must be NEXT Friday.
        now_utc = datetime(2026, 4, 24, 15, 0, 0, tzinfo=UTC)  # 20:00 Almaty Friday
        out = SchedulerLoop._next_slot_occurrence(
            now_utc, Weekday.FRIDAY, time(18, 0)
        )
        assert out == datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY)

    def test_next_slot_same_day_future_time_uses_today(self) -> None:
        # Friday 10:00 Almaty = 05:00 UTC; slot Friday 18:00 → today 18:00.
        now_utc = datetime(2026, 4, 24, 5, 0, 0, tzinfo=UTC)
        out = SchedulerLoop._next_slot_occurrence(
            now_utc, Weekday.FRIDAY, time(18, 0)
        )
        assert out == datetime(2026, 4, 24, 18, 0, tzinfo=ALMATY)

    async def test_default_constructor_uses_system_clock_and_check(
        self,
        fake_client: Callable[..., FakeAltegioClient],
    ) -> None:
        # Smoke: constructing the loop without clock/ntp_checker overrides should not raise
        # and should wire SystemClock + default ntp_checker (which is a partial of check_ntp_drift).
        client = fake_client([])
        loop = SchedulerLoop(
            config=_config(),
            altegio_client=as_altegio_client(client),
        )
        assert loop._clock is not None
        assert loop._ntp_checker is not None

    async def test_default_ntp_checker_invokes_check_ntp_drift(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Sanity: construct with default ntp_checker and verify that calling it dispatches
        # to check_ntp_drift with the configured threshold.
        from tennis_booking.scheduler import loop as loop_module

        captured: dict[str, Any] = {}

        async def _fake_check(**kwargs: Any) -> Any:
            captured.update(kwargs)
            from tennis_booking.scheduler.clock import CheckResult

            return CheckResult(
                server="fake", ntp_time=datetime.now(UTC), drift_ms=1.0, rtt_ms=1.0
            )

        monkeypatch.setattr(loop_module, "check_ntp_drift", _fake_check)
        client = fake_client([])
        loop = SchedulerLoop(
            config=_config(),
            altegio_client=as_altegio_client(client),
            ntp_threshold_ms=77,
        )
        await loop._ntp_checker()
        assert captured.get("threshold_ms") == 77

    async def test_idempotency_on_recompute_no_duplicate_tasks(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Window 1h in future — task spawned but won't fire during test.
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 0, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)

        sched1 = await loop._recompute_windows(clock.now_utc())
        loop._spawn_attempts(sched1)
        assert len(loop._scheduled) == 1
        await asyncio.sleep(0)

        sched2 = await loop._recompute_windows(clock.now_utc())
        loop._spawn_attempts(sched2)
        assert len(loop._scheduled) == 1
        # Cleanup
        await loop.stop()


# ---------- 4. Attempt launch -----------------------------------------------


class TestAttemptLaunch:
    async def test_happy_path_full_cycle(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Window 5 min in future. Loop spawns a task that sleeps to T-30s,
        # then runs FakeBookingAttempt which returns success immediately.
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        factory, created = fake_attempt_factory(
            result=AttemptResult(
                status="won",
                booking=BookingResponse(record_id=99, record_hash="h99"),
                duplicates=(),
                fired_at_utc=now_utc,
                response_at_utc=now_utc,
                duration_ms=10.0,
                business_code=None,
                transport_cause=None,
                prearm_ok=True,
                shots_fired=1,
                attempt_id="fake-99",
            )
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)

        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)

        # Drive the wait_and_attempt task to completion.
        task = next(iter(loop._scheduled.values()))
        # window in 5min, prearm_lead 30s → should sleep ~270s then run.
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done(), "wait_and_attempt did not finish"
        assert len(created) == 1
        assert created[0].run_calls == [sched[0].window_open_utc]
        await loop.stop()

    async def test_attempt_called_with_correct_config(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        prof = Profile(name="alice", full_name="Alice A", phone="77777", email="a@x.com")
        cfg = _config(_booking(name="b", court_id=1234, profile=prof))
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)

        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert created
        ac: AttemptConfig = created[0].config
        assert ac.court_id == 1234
        assert ac.fullname == "Alice A"
        assert ac.phone == "77777"
        assert ac.email == "a@x.com"
        assert ac.slot_dt_local == sched[0].slot_dt_local
        await loop.stop()

    async def test_pre_attempt_ntp_drift_warn_not_fatal(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        ok_ntp_check: Callable[..., Any],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking())
        # Drift > threshold (150ms vs 50ms default). Single effect — used for pre-attempt
        # (bypassing startup). Covers the explicit threshold-compare branch.
        ntp = fake_ntp_checker(effects=[ok_ntp_check(150.0)], loop=True)
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ntp, attempt_factory=factory)

        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        assert len(created) == 1, "attempt must still run despite NTP drift warn"
        await loop.stop()

    async def test_attempt_returns_lost_logged_with_business_code(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking())
        factory, created = fake_attempt_factory(
            result=AttemptResult(
                status="lost",
                booking=None,
                duplicates=(),
                fired_at_utc=now_utc,
                response_at_utc=now_utc,
                duration_ms=20.0,
                business_code="slot_taken",
                transport_cause=None,
                prearm_ok=True,
                shots_fired=2,
                attempt_id="fake-x",
            )
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        assert created and created[0].run_calls
        await loop.stop()

    async def test_attempt_raises_unexpected_logged_loop_continues(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking(name="boom"))
        factory, created = fake_attempt_factory(raise_exc=RuntimeError("kaboom"))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        # Task shouldn't propagate exception — loop swallows it
        assert task.exception() is None
        await loop.stop()


# ---------- 5. Graceful shutdown --------------------------------------------


class TestGracefulShutdown:
    async def test_stop_no_active_attempts_returns_quickly(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        await loop.stop()  # idempotent on fresh instance
        assert loop._scheduled == {}

    async def test_stop_attempts_in_scheduled_phase_cancelled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Window 24h in future — task will sleep almost forever, never reach prearm.
        clock = make_clock(initial_utc=datetime(2026, 4, 18, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)
        sched = await loop._recompute_windows(clock.now_utc())
        loop._spawn_attempts(sched)
        await asyncio.sleep(0)
        await loop.stop()
        # Attempt was never invoked (cancelled in scheduled phase)
        assert created == []

    async def test_stop_idempotent_double_call(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        await loop.stop()
        await loop.stop()
        await loop.stop()

    async def test_stop_running_phase_waits_for_attempt(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Window 5 min in future → task reaches prearm quickly and enters "running" phase.
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        factory, created = fake_attempt_factory(delay_s=1.0)
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))

        # Advance clock enough that task crosses into running phase (past prearm sleep)
        for _ in range(50):
            await asyncio.sleep(0)
            if task in loop._running:
                break
            clock.advance(10.0)
        assert task in loop._running, "task did not reach running phase"

        # Now stop — the task in running phase should be awaited
        stop_task = asyncio.create_task(loop.stop())
        for _ in range(20):
            await asyncio.sleep(0)
            clock.advance(1.0)
            if stop_task.done():
                break
        assert stop_task.done()
        await stop_task
        assert created and created[0].run_calls


# ---------- 5b. Wait_or_stop timeout ----------------------------------------


class TestWaitOrStop:
    async def test_wait_or_stop_returns_on_timeout(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        # Very short delay — should timeout and return.
        await loop._wait_or_stop(0.01)

    async def test_wait_or_stop_returns_immediately_on_stop_event(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        loop._stop_event.set()
        await loop._wait_or_stop(5.0)  # returns early via stop_event

    async def test_wait_or_stop_zero_delay(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        await loop._wait_or_stop(0.0)


# ---------- 5c. Pre-attempt NTP error paths ---------------------------------


class TestPreAttemptNTPErrorPaths:
    async def test_pre_attempt_ntp_drift_error_does_not_fail(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking())
        # Only one effect — ClockDriftError. Used for pre-attempt check (startup is bypassed).
        ntp = fake_ntp_checker(
            effects=[ClockDriftError(200.0, 50, "srv")],
            loop=True,
        )
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ntp, attempt_factory=factory)
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        assert len(created) == 1
        await loop.stop()

    async def test_pre_attempt_ntp_unreachable_does_not_fail(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking())
        ntp = fake_ntp_checker(
            effects=[NTPUnreachableError("srv", "timeout")],
            loop=True,
        )
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ntp, attempt_factory=factory)
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        assert len(created) == 1
        await loop.stop()

    async def test_pre_attempt_ntp_generic_exception_does_not_fail(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        fake_ntp_checker: Callable[..., FakeNTPChecker],
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(_booking())
        ntp = fake_ntp_checker(
            effects=[RuntimeError("unexpected")],
            loop=True,
        )
        factory, created = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ntp, attempt_factory=factory)
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        assert len(created) == 1
        await loop.stop()


# ---------- 6. Error handling -----------------------------------------------


class TestErrorHandling:
    async def test_recompute_raises_loop_continues(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking())
        factory, _ = fake_attempt_factory()
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)

        async def boom(_: datetime) -> Any:
            raise RuntimeError("recompute error")

        monkeypatch.setattr(loop, "_recompute_windows", boom)
        # Drive run() briefly
        run_task = asyncio.create_task(loop.run())
        for _ in range(5):
            await asyncio.sleep(0)
        await loop.stop()
        await run_task

    async def test_attempt_factory_raises_other_attempts_ok(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(
            _booking(name="b1", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)),
            _booking(name="b2", weekday=Weekday.FRIDAY, slot_local_time=time(19, 0)),
        )

        # Factory raises only for b2
        from tennis_booking.engine.attempt import BookingAttempt as RealBA  # noqa: N814
        from tests.scheduler.conftest import FakeBookingAttempt

        def factory(c: AttemptConfig, cl: Any, ck: Any) -> RealBA:
            if c.slot_dt_local.hour == 19:
                raise RuntimeError("factory boom for b2")
            inst = FakeBookingAttempt(c, cl, ck)
            return inst  # type: ignore[return-value]

        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory)
        sched = await loop._recompute_windows(now_utc)
        loop._spawn_attempts(sched)

        for _ in range(60):
            await asyncio.sleep(0)
            clock.advance(10.0)
            if all(t.done() for t in loop._scheduled.values()):
                break
        for t in loop._scheduled.values():
            # both tasks completed without propagating
            assert t.done()
            assert t.exception() is None
        await loop.stop()


# ---------- 7. AttemptConfig wiring -----------------------------------------


class TestAttemptConfigBuild:
    async def test_build_attempt_config_uses_profile_fields(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        prof = Profile(
            name="bob", full_name="Bob B", phone="77002223344", email=None
        )
        b = _booking(name="x", profile=prof, court_id=4242)
        loop = _build_loop(_config(b), clock, client, ntp_checker=ok_ntp_checker)
        sa = (await loop._recompute_windows(clock.now_utc()))
        # If no scheduled (window past), build a synthetic ScheduledAttempt
        if not sa:
            from tennis_booking.scheduler.loop import ScheduledAttempt
            slot_local = datetime(2030, 1, 4, 18, 0, tzinfo=ALMATY)  # future Friday
            sa = [ScheduledAttempt(
                booking=b,
                slot_dt_local=slot_local,
                window_open_utc=_almaty_to_utc(datetime(2030, 1, 1, 7, 0, tzinfo=ALMATY)),
            )]
        cfg_out = loop._build_attempt_config(sa[0])
        assert cfg_out.court_id == 4242
        assert cfg_out.fullname == "Bob B"
        assert cfg_out.phone == "77002223344"
        assert cfg_out.email is None


# ---------- 8. Integration --------------------------------------------------


class TestIntegration:
    async def test_e2e_dry_run_won(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # Use real BookingAttempt + FakeAltegioClient with dry_run=True;
        # client returns BookingResponse(record_hash="dry-run") immediately for any call.
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client(
            [BookingResponse(record_id=0, record_hash="dry-run")] * 4,
            dry_run=True,
        )
        cfg = _config(_booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0), court_id=SERVICE_ID))

        # No attempt_factory override → real BookingAttempt used.
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        sched = await loop._recompute_windows(now_utc)
        assert len(sched) == 1
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))

        for _ in range(2000):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(0.5)
        assert task.done(), "real BookingAttempt did not finish in time-budget"
        assert task.exception() is None
        await loop.stop()
