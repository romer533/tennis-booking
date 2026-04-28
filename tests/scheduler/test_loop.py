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
    CourtPool,
    Profile,
    ResolvedBooking,
    Weekday,
)
from tennis_booking.engine.attempt import AttemptConfig, AttemptResult
from tennis_booking.scheduler.clock_errors import ClockDriftError, NTPUnreachableError
from tennis_booking.scheduler.loop import (
    DEFAULT_NTP_THRESHOLD_MS,
    LOOKAHEAD_WEEKS,
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
    court_id: int | None = None,
    court_ids: tuple[int, ...] | None = None,
    service_id: int = SERVICE_ID,
    profile: Profile | None = None,
    enabled: bool = True,
    duration_minutes: int = 60,
    pool_name: str | None = None,
) -> ResolvedBooking:
    if court_ids is None:
        court_ids = (court_id if court_id is not None else STAFF_ID,)
    return ResolvedBooking(
        name=name,
        weekday=weekday,
        slot_local_time=slot_local_time,
        duration_minutes=duration_minutes,
        court_ids=court_ids,
        service_id=service_id,
        profile=profile or _profile(),
        enabled=enabled,
        pool_name=pool_name,
    )


def _config(
    *bookings: ResolvedBooking,
    court_pools: dict[str, CourtPool] | None = None,
) -> AppConfig:
    profiles_by_name: dict[str, Profile] = {}
    for b in bookings:
        profiles_by_name.setdefault(b.profile.name, b.profile)
    return AppConfig(
        bookings=tuple(bookings),
        profiles=MappingProxyType(profiles_by_name),
        court_pools=MappingProxyType(dict(court_pools or {})),
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

    async def test_next_recompute_at_06_55_00_returns_tomorrow(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # Exact-match semantics changed: today's recompute is "already done"
        # by the run() startup-immediate path, so _next_recompute_at returns
        # tomorrow's slot — this avoids tight-loop on FakeClock at 06:55:00.
        clock = make_clock(initial_utc=datetime(2026, 4, 24, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        target = loop._next_recompute_at(clock.now_utc())
        delta = (target - clock.now_utc()).total_seconds()
        # Tomorrow 06:55 Almaty → exactly 24h ahead.
        assert delta == pytest.approx(86400.0, abs=0.01)
        target_local = target.astimezone(ALMATY)
        assert target_local.hour == 6 and target_local.minute == 55

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
        # Wednesday 2026-04-22 06:55 Almaty = 01:55 UTC.
        # Friday 18:00 Almaty slot opens Wednesday 07:00 → in 5 minutes.
        clock = make_clock(initial_utc=datetime(2026, 4, 22, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(name="fri", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.booking.name == "fri"
        # Slot is Friday 2026-04-24 18:00 Almaty (this Friday)
        assert sa.slot_dt_local == datetime(2026, 4, 24, 18, 0, tzinfo=ALMATY)
        # Window opens Wednesday 2026-04-22 07:00 Almaty = 02:00 UTC
        assert sa.window_open_utc == datetime(2026, 4, 22, 2, 0, tzinfo=UTC)

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
        # 07:30 Almaty Wednesday = 02:30 UTC. Nearest Friday 18:00 slot's window opened
        # at Wednesday 07:00 → already 30 min ago → that occurrence is skipped with
        # window_passed warn, but the loop must roll forward to the *next* Friday
        # (2026-05-01) whose window opens 2026-04-29 02:00 UTC — in the future.
        clock = make_clock(initial_utc=datetime(2026, 4, 22, 2, 30, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(name="fri", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.slot_dt_local == datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY)
        assert sa.window_open_utc == datetime(2026, 4, 29, 2, 0, tzinfo=UTC)

    async def test_friday_18_slot_recompute_wednesday_06_55(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # Wednesday 06:55 Almaty = 01:55 UTC, slot Fri 18:00 → window in 5 min
        clock = make_clock(initial_utc=datetime(2026, 4, 22, 1, 55, 0, tzinfo=UTC))
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
        # 2026-01-01 was Thursday. Slot Thursday 12:00, "now" is Tue 2025-12-30 06:55
        # Almaty = Tue 2025-12-30 01:55 UTC. Window: T-2 = Tue 2025-12-30 07:00 Almaty.
        clock = make_clock(initial_utc=datetime(2025, 12, 30, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        cfg = _config(_booking(weekday=Weekday.THURSDAY, slot_local_time=time(12, 0)))
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        scheduled = await loop._recompute_windows(clock.now_utc())
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.slot_dt_local == datetime(2026, 1, 1, 12, 0, tzinfo=ALMATY)
        assert sa.window_open_utc == datetime(2025, 12, 30, 2, 0, tzinfo=UTC)

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
        cfg = _config(_booking(name="b", court_id=1234, service_id=4242, profile=prof))
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
        assert ac.court_ids == (1234,)
        assert ac.service_id == 4242
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
        b = _booking(name="x", profile=prof, court_id=4242, service_id=9999)
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
        assert cfg_out.court_ids == (4242,)
        assert cfg_out.service_id == 9999
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
        cfg = _config(
            _booking(
                weekday=Weekday.FRIDAY,
                slot_local_time=time(18, 0),
                court_id=STAFF_ID,
                service_id=SERVICE_ID,
            )
        )

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


# ---------- 9. CR regression: scheduled_key collision -----------------------


class TestScheduledKeyCollision:
    async def test_two_bookings_same_name_different_court_both_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Two BookingRule share name="Вечер" but differ by court_id. This is legal:
        # loader dedupes by (weekday, slot_time, court_id), not by name. Before the
        # fix the second one was silently dropped as "duplicate".
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)  # Tue 06:55 Almaty
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        prof = _profile("roman")
        cfg = _config(
            _booking(
                name="Вечер",
                weekday=Weekday.FRIDAY,
                slot_local_time=time(18, 0),
                court_id=5,
                service_id=SERVICE_ID,
                profile=prof,
            ),
            _booking(
                name="Вечер",
                weekday=Weekday.FRIDAY,
                slot_local_time=time(18, 0),
                court_id=6,
                service_id=SERVICE_ID,
                profile=prof,
            ),
        )
        factory, created = fake_attempt_factory()
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )

        sched = await loop._recompute_windows(now_utc)
        assert len(sched) == 2, "recompute must produce two ScheduledAttempt"
        loop._spawn_attempts(sched)
        assert len(loop._scheduled) == 2, (
            "both same-name different-court bookings must be scheduled; "
            "if only 1, the key is still colliding"
        )

        # Drive both tasks to completion to confirm both BookingAttempts run.
        tasks = list(loop._scheduled.values())
        for _ in range(50):
            if all(t.done() for t in tasks):
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert all(t.done() for t in tasks)
        assert len(created) == 2
        # Each AttemptConfig has court_ids as a single-element tuple in legacy mode.
        first_courts = sorted(inst.config.court_ids[0] for inst in created)
        assert first_courts == [5, 6]
        await loop.stop()

    async def test_two_bookings_same_name_different_service_both_scheduled(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Same court_id, same name, but different service_id — service_id is part
        # of the key for maximum conservatism. Note: loader normally dedupes
        # (weekday, slot, court_id), so this combo wouldn't pass loader validation,
        # but the key must still distinguish them for in-memory correctness.
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        prof = _profile("roman")
        b1 = _booking(
            name="same", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0),
            court_id=10, service_id=111, profile=prof,
        )
        b2 = _booking(
            name="same", weekday=Weekday.FRIDAY, slot_local_time=time(18, 0),
            court_id=10, service_id=222, profile=prof,
        )
        cfg = _config(b1, b2)
        factory, _created = fake_attempt_factory()
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )

        sched = await loop._recompute_windows(now_utc)
        assert len(sched) == 2
        loop._spawn_attempts(sched)
        assert len(loop._scheduled) == 2, (
            "service_id must also participate in dedup key"
        )
        await loop.stop()


# ---------- 10. CR regression: tight-loop at exact recompute time -----------


class TestTightLoopAtExactRecompute:
    async def test_start_at_06_55_00_recompute_immediately_then_tomorrow(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # 06:55:00 Almaty = 01:55:00 UTC. Start the loop at the exact daily
        # recompute time. Before the fix this was an infinite tight loop on
        # FakeClock (delay_s → 0, recompute, delay_s → 0, ...).
        # Empty config keeps the clock stable (no spawned task sleeps).
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config()
        factory, _created = fake_attempt_factory()

        call_count = {"n": 0}

        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )
        real_recompute = loop._recompute_windows

        async def counting_recompute(now: datetime) -> list[Any]:
            call_count["n"] += 1
            return await real_recompute(now)

        loop._recompute_windows = counting_recompute  # type: ignore[method-assign]

        run_task = asyncio.create_task(loop.run())
        # Let startup NTP + first immediate recompute land.
        for _ in range(10):
            await asyncio.sleep(0)
        assert call_count["n"] == 1, (
            "immediate recompute at startup must have run exactly once"
        )

        # Clock hasn't moved (empty config, no sleeps) — next recompute is +24h.
        assert clock.now_utc() == now_utc
        next_at = loop._next_recompute_at(clock.now_utc())
        delta = (next_at - clock.now_utc()).total_seconds()
        assert delta == pytest.approx(86400.0, abs=0.01)

        await loop.stop()
        await run_task

    async def test_start_at_06_54_59_recompute_immediately(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Just-before-recompute start: first recompute still fires immediately
        # (not waiting 1s), then next is today's 06:55 (1s ahead).
        now_utc = datetime(2026, 4, 21, 1, 54, 59, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        # Empty config: clock stays still, we can inspect _next_recompute_at cleanly.
        cfg = _config()
        factory, _created = fake_attempt_factory()

        call_count = {"n": 0}
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )
        real_recompute = loop._recompute_windows

        async def counting_recompute(now: datetime) -> list[Any]:
            call_count["n"] += 1
            return await real_recompute(now)

        loop._recompute_windows = counting_recompute  # type: ignore[method-assign]

        run_task = asyncio.create_task(loop.run())
        for _ in range(10):
            await asyncio.sleep(0)
        assert call_count["n"] == 1, (
            "first recompute must run immediately at startup regardless of time"
        )

        # The next-target is today's 06:55 — 1s ahead.
        next_at = loop._next_recompute_at(clock.now_utc())
        delta = (next_at - clock.now_utc()).total_seconds()
        assert delta == pytest.approx(1.0, abs=0.01)

        await loop.stop()
        await run_task

    async def test_start_at_07_00_recompute_immediately_windows_skipped(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # 07:30 Almaty Wednesday = 02:30 UTC. The nearest Friday-18:00 booking's
        # window opens Wed 07:00 — already in the past. The loop must roll
        # forward to next Friday (2026-05-01) whose window opens 2026-04-29
        # 02:00 UTC — well in the future, so a task IS spawned (sleeping toward
        # that prearm). Next scheduled recompute must still be tomorrow 06:55
        # (not today's already-passed 06:55 — that would be a tight loop).
        now_utc = datetime(2026, 4, 22, 2, 30, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(
            _booking(weekday=Weekday.FRIDAY, slot_local_time=time(18, 0)),
        )
        factory, created = fake_attempt_factory()
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )

        run_task = asyncio.create_task(loop.run())
        for _ in range(10):
            await asyncio.sleep(0)
        # Recompute ran; nearest window was past, so loop rolled forward to
        # next-week occurrence and spawned an attempt — the FakeClock auto-
        # advances inside sleep(), so the task ran end-to-end. Exactly one
        # FakeBookingAttempt must have been invoked (proving the schedule was
        # for the future-week occurrence, not skipped). Before the fix this
        # was always 0 because the only candidate was past and skipped.
        assert len(created) == 1
        # And the schedule fired for the next-week slot (2026-05-01 18:00 Almaty),
        # not the nearest already-passed occurrence.
        assert created[0].config.slot_dt_local == datetime(
            2026, 5, 1, 18, 0, tzinfo=ALMATY
        )

        await loop.stop()
        await run_task

    async def test_loop_does_not_spin_at_exact_match_without_clock_advance(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # Direct tight-loop regression: start exactly at recompute time, do NOT
        # advance the FakeClock, give the loop several scheduler ticks, then
        # ensure recompute was invoked exactly once (startup-immediate) and the
        # loop is parked waiting on _stop_event / timeout — not re-invoking
        # recompute in a hot spin.
        now_utc = datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config()  # empty → recompute is cheap, fast to count invocations
        factory, _created = fake_attempt_factory()
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )

        call_count = {"n": 0}
        real_recompute = loop._recompute_windows

        async def counting_recompute(now: datetime) -> list[Any]:
            call_count["n"] += 1
            return await real_recompute(now)

        loop._recompute_windows = counting_recompute  # type: ignore[method-assign]

        run_task = asyncio.create_task(loop.run())
        # Yield many times — without fix the loop would spin here, incrementing
        # call_count arbitrarily many times in a single sync block.
        for _ in range(100):
            await asyncio.sleep(0)

        assert call_count["n"] == 1, (
            f"recompute must run exactly once at exact-match startup, got {call_count['n']}"
        )

        await loop.stop()
        await run_task


# ---------- 11. Nit: shutdown_timeout_s injectable --------------------------


class TestShutdownTimeoutInjectable:
    async def test_custom_shutdown_timeout_propagates(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = SchedulerLoop(
            config=_config(),
            altegio_client=as_altegio_client(client),
            clock=as_clock(clock),
            ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
            shutdown_timeout_s=5.0,
        )
        assert loop._shutdown_timeout_s == 5.0
        # And stop() completes cleanly with the injected value.
        await loop.stop()

    async def test_default_shutdown_timeout_is_60s(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        loop = _build_loop(_config(), clock, client, ntp_checker=ok_ntp_checker)
        assert loop._shutdown_timeout_s == 60.0


# ---------- 12. Court pool: booking → loop → AttemptConfig ------------------


class TestCourtPoolE2EViaLoop:
    async def test_court_pool_booking_propagates_court_ids_to_attempt(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # ResolvedBooking with court_ids tuple from a pool — loop must build
        # AttemptConfig with the same tuple, not collapse it.
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        prof = _profile("roman")
        b = _booking(
            name="pool-booking",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            court_ids=(101, 102, 103),
            service_id=7849893,
            profile=prof,
            pool_name="indoor",
        )
        cfg = _config(
            b,
            court_pools={
                "indoor": CourtPool(service_id=7849893, court_ids=(101, 102, 103))
            },
        )
        factory, created = fake_attempt_factory()
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )

        sched = await loop._recompute_windows(clock.now_utc())
        assert len(sched) == 1
        loop._spawn_attempts(sched)
        task = next(iter(loop._scheduled.values()))
        for _ in range(50):
            if task.done():
                break
            await asyncio.sleep(0)
            clock.advance(10.0)
        assert task.done()
        assert len(created) == 1
        ac = created[0].config
        assert ac.court_ids == (101, 102, 103)
        assert ac.service_id == 7849893
        await loop.stop()

    async def test_pool_booking_scheduled_key_distinct_from_legacy(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        fake_attempt_factory: Callable[..., Any],
    ) -> None:
        # A pool booking and a legacy single-court booking targeting the same
        # name/slot/service but different court_ids must not collide in
        # _scheduled (their hash(court_ids) differ).
        clock = make_clock(initial_utc=datetime(2026, 4, 21, 1, 55, 0, tzinfo=UTC))
        client = fake_client([])
        prof = _profile("roman")
        b1 = _booking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            court_ids=(50, 60),
            service_id=7849893,
            profile=prof,
            pool_name="p",
        )
        b2 = _booking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            court_ids=(99,),
            service_id=7849893,
            profile=prof,
        )
        cfg = _config(
            b1, b2,
            court_pools={"p": CourtPool(service_id=7849893, court_ids=(50, 60))},
        )
        factory, _created = fake_attempt_factory()
        loop = _build_loop(
            cfg, clock, client, ntp_checker=ok_ntp_checker, attempt_factory=factory
        )
        sched = await loop._recompute_windows(clock.now_utc())
        loop._spawn_attempts(sched)
        assert len(loop._scheduled) == 2, (
            "pool+legacy with different court_ids must be tracked as separate keys"
        )
        await loop.stop()


# ---------- 13. Iterate to next weekly occurrence when nearest passed -------


class _RecordingLogger:
    """Drop-in for structlog BoundLogger that records every method call.

    The loop only ever calls .info/.warning/.error/.exception/.bind on its logger.
    `bind(...)` returns a child that shares the same `events` list, so kwargs
    bound at call sites are merged into the recorded event for assertion.
    """

    def __init__(
        self,
        events: list[tuple[str, str, dict[str, Any]]] | None = None,
        bound: dict[str, Any] | None = None,
    ) -> None:
        self.events: list[tuple[str, str, dict[str, Any]]] = (
            events if events is not None else []
        )
        self._bound: dict[str, Any] = dict(bound or {})

    def bind(self, **kwargs: Any) -> _RecordingLogger:
        merged = {**self._bound, **kwargs}
        return _RecordingLogger(events=self.events, bound=merged)

    def _record(self, level: str, event: str, **kwargs: Any) -> None:
        merged = {**self._bound, **kwargs}
        self.events.append((level, event, merged))

    def info(self, event: str, **kwargs: Any) -> None:
        self._record("info", event, **kwargs)

    def warning(self, event: str, **kwargs: Any) -> None:
        self._record("warning", event, **kwargs)

    def error(self, event: str, **kwargs: Any) -> None:
        self._record("error", event, **kwargs)

    def exception(self, event: str, **kwargs: Any) -> None:
        self._record("error", event, **kwargs)

    def by_event(self, event: str) -> list[dict[str, Any]]:
        return [kw for _lvl, ev, kw in self.events if ev == event]


class TestWindowPassedSkipsToNextWeek:
    async def test_nearest_occurrence_window_already_passed_skips_to_next_week(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # Reproduces the prod incident:
        # weekday=sunday, slot_local_time="23:00" Almaty.
        # Service started 2026-04-24 19:17 UTC (Friday evening).
        # Nearest Sunday = 2026-04-26 23:00 Almaty → window opens 2026-04-24
        # 07:00 Almaty = 2026-04-24 02:00 UTC → ALREADY PAST (today, 17 h ago).
        # The loop must roll forward to the next Sunday (2026-05-03 23:00 Almaty)
        # whose window opens 2026-05-01 02:00 UTC.
        now_utc = datetime(2026, 4, 24, 19, 17, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(
            _booking(
                name="sun-night",
                weekday=Weekday.SUNDAY,
                slot_local_time=time(23, 0),
            )
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        recorder = _RecordingLogger()
        loop._log = recorder  # type: ignore[assignment]

        scheduled = await loop._recompute_windows(now_utc)
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.slot_dt_local == datetime(2026, 5, 3, 23, 0, tzinfo=ALMATY)
        assert sa.window_open_utc == datetime(2026, 5, 1, 2, 0, tzinfo=UTC)

        # window_passed must have been logged exactly once for the skipped
        # nearest occurrence, and recompute_done must report scheduled=1.
        passed = recorder.by_event("window_passed")
        assert len(passed) == 1
        assert passed[0]["booking_name"] == "sun-night"
        assert passed[0]["slot_dt_local"] == (
            datetime(2026, 4, 26, 23, 0, tzinfo=ALMATY).isoformat()
        )

        done = recorder.by_event("recompute_done")
        assert done and done[-1]["scheduled"] == 1

        # No sanity-error must fire on a healthy config.
        assert recorder.by_event("no_future_window_found") == []

    async def test_nearest_occurrence_window_in_future_uses_nearest(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
    ) -> None:
        # 2026-04-20 00:00 UTC = 05:00 Almaty Monday. Nearest Sunday 23:00 slot
        # = 2026-04-26 23:00 Almaty → window opens 2026-04-24 07:00 Almaty
        # = 2026-04-24 02:00 UTC → still in the future. Use the nearest occurrence.
        now_utc = datetime(2026, 4, 20, 0, 0, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(
            _booking(
                name="sun-night",
                weekday=Weekday.SUNDAY,
                slot_local_time=time(23, 0),
            )
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        recorder = _RecordingLogger()
        loop._log = recorder  # type: ignore[assignment]

        scheduled = await loop._recompute_windows(now_utc)
        assert len(scheduled) == 1
        sa = scheduled[0]
        assert sa.slot_dt_local == datetime(2026, 4, 26, 23, 0, tzinfo=ALMATY)
        assert sa.window_open_utc == datetime(2026, 4, 24, 2, 0, tzinfo=UTC)
        # No window_passed warns when nearest occurrence is already valid.
        assert recorder.by_event("window_passed") == []

    async def test_all_occurrences_past_within_lookahead_logs_error(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Sanity guard: if next_open_window keeps returning past values for every
        # candidate within LOOKAHEAD_WEEKS (impossible for a healthy config,
        # but defensive), the loop must log no_future_window_found and skip
        # the booking — never hang, never schedule a stale window.
        now_utc = datetime(2026, 4, 24, 19, 17, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(
            _booking(
                name="sun-night",
                weekday=Weekday.SUNDAY,
                slot_local_time=time(23, 0),
            )
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        recorder = _RecordingLogger()
        loop._log = recorder  # type: ignore[assignment]

        from tennis_booking.scheduler import loop as loop_module

        # Always return a past timestamp regardless of input.
        past_utc = datetime(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
        monkeypatch.setattr(loop_module, "next_open_window", lambda _slot: past_utc)

        scheduled = await loop._recompute_windows(now_utc)
        assert scheduled == []

        # window_passed must be warned once per checked week, then a single
        # no_future_window_found error.
        passed = recorder.by_event("window_passed")
        assert len(passed) == LOOKAHEAD_WEEKS
        errors = recorder.by_event("no_future_window_found")
        assert len(errors) == 1
        assert errors[0]["booking_name"] == "sun-night"
        assert errors[0]["weeks_searched"] == LOOKAHEAD_WEEKS

        done = recorder.by_event("recompute_done")
        assert done and done[-1]["scheduled"] == 0

    async def test_multiple_window_passed_warns_before_scheduling(
        self,
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        ok_ntp_checker: FakeNTPChecker,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Force the first three patched-window calls past, the fourth in the future.
        # The first call comes from the post-window restart-resilience pre-check
        # (added in `feat/post-window-poll`); the next two are inner-loop iterations
        # that produce the `window_passed` warns; the fourth resolves to a future
        # window. Expected: two `window_passed` warns, then a successful schedule.
        now_utc = datetime(2026, 4, 24, 19, 17, 0, tzinfo=UTC)
        clock = make_clock(initial_utc=now_utc)
        client = fake_client([])
        cfg = _config(
            _booking(
                name="sun-night",
                weekday=Weekday.SUNDAY,
                slot_local_time=time(23, 0),
            )
        )
        loop = _build_loop(cfg, clock, client, ntp_checker=ok_ntp_checker)
        recorder = _RecordingLogger()
        loop._log = recorder  # type: ignore[assignment]

        from tennis_booking.scheduler import loop as loop_module

        past_utc = datetime(2020, 1, 1, 0, 0, 0, tzinfo=UTC)
        future_utc = datetime(2030, 1, 1, 0, 0, 0, tzinfo=UTC)
        call_count = {"n": 0}

        def fake_window(_slot: datetime) -> datetime:
            call_count["n"] += 1
            return past_utc if call_count["n"] <= 3 else future_utc

        monkeypatch.setattr(loop_module, "next_open_window", fake_window)

        scheduled = await loop._recompute_windows(now_utc)
        assert len(scheduled) == 1
        assert scheduled[0].window_open_utc == future_utc

        # Exactly two skipped occurrences logged before success.
        assert len(recorder.by_event("window_passed")) == 2
        assert recorder.by_event("no_future_window_found") == []

    def test_lookahead_weeks_constant(self) -> None:
        # Guard against accidental tuning. Bumping LOOKAHEAD_WEEKS should be
        # an explicit decision (and require updating this test).
        assert LOOKAHEAD_WEEKS == 4
