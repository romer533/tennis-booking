"""SchedulerLoop — shared poll-result cache wiring.

Structural tests verifying that the loop maintains exactly one PollResultCache
instance across its lifetime and feeds it to every PollAttempt the default
factories build. This is what collapses the production 21-fetches-per-cycle
to 3 (one per active date).

Also covers the jitter contract: spawned PollAttempt instances perform an
initial jitter sleep before the first tick so synchronised bursts are spread.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from types import MappingProxyType

from tennis_booking.altegio import (
    BookingResponse,
)
from tennis_booking.common.tz import ALMATY
from tennis_booking.config.schema import (
    AppConfig,
    PollConfig,
    Profile,
    ResolvedBooking,
    Weekday,
)
from tennis_booking.engine.attempt import AttemptConfig
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.engine.poll_cache import PollResultCache
from tennis_booking.scheduler.loop import (
    DEFAULT_NTP_THRESHOLD_MS,
    DEFAULT_POST_WINDOW_POLL_INTERVAL_S,
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


def _profile(name: str = "roman") -> Profile:
    return Profile(name=name, full_name="Roman G", phone="77001234567", email=None)


def _booking(
    name: str = "fri-eve",
    profile: Profile | None = None,
    court_ids: tuple[int, ...] = (STAFF_ID,),
    pool_name: str | None = None,
    poll: PollConfig | None = None,
) -> ResolvedBooking:
    return ResolvedBooking(
        name=name,
        weekday=Weekday.FRIDAY,
        slot_local_time=time(18, 0),
        duration_minutes=60,
        court_ids=court_ids,
        service_id=SERVICE_ID,
        profile=profile or _profile(),
        enabled=True,
        pool_name=pool_name,
        poll=poll,
    )


def _config(*bookings: ResolvedBooking) -> AppConfig:
    profiles = {b.profile.name: b.profile for b in bookings}
    return AppConfig(
        bookings=tuple(bookings),
        profiles=MappingProxyType(profiles),
        court_pools=MappingProxyType({}),
    )


def test_scheduler_loop_creates_single_cache_shared_across_polls(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """The loop owns exactly one PollResultCache instance, exposed to default
    factories via closure. Verifies the cache is the same identity across
    multiple PollAttempt builds.
    """
    cfg = _config(_booking())
    clock = make_clock()
    client = fake_client([])
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
    )
    cache_a = loop._poll_cache
    assert isinstance(cache_a, PollResultCache)

    # Build two PollAttempts via the default factories — they should both
    # close over the SAME cache instance.
    attempt_cfg = AttemptConfig(
        slot_dt_local=datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY),
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77001234567",
        profile_name="roman",
        pool_key="evening",
    )
    poll_data = PollConfigData(interval_s=60, start_offset_days=2)
    won = asyncio.Event()
    a = loop._poll_attempt_factory(
        attempt_cfg, poll_data, as_altegio_client(client), as_clock(clock), won, None
    )
    b = loop._poll_attempt_factory(
        attempt_cfg, poll_data, as_altegio_client(client), as_clock(clock), won, None
    )
    # Both PollAttempt instances reference the same cache (closure capture).
    assert a._cache is b._cache
    assert a._cache is loop._poll_cache


def test_scheduler_loop_post_window_uses_same_cache(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """Post-window factory shares the SAME cache as the in-window poll factory.

    Important: in-window poll and post-window poll for the same booking-occurrence
    hit the same (date, pool) endpoint — they MUST share a cache or we'd double
    the load again.
    """
    cfg = _config(_booking())
    clock = make_clock()
    client = fake_client([])
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
    )
    attempt_cfg = AttemptConfig(
        slot_dt_local=datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY),
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77001234567",
        profile_name="roman",
        pool_key="evening",
    )
    poll_data = PollConfigData(interval_s=60, start_offset_days=2)
    won = asyncio.Event()
    pre = loop._poll_attempt_factory(
        attempt_cfg, poll_data, as_altegio_client(client), as_clock(clock), won, None
    )
    post = loop._post_window_poll_factory(
        attempt_cfg, poll_data, as_altegio_client(client), as_clock(clock), won, None
    )
    assert pre._cache is post._cache is loop._poll_cache
    # Sanity: post-window mode flag
    assert post._post_window_mode is True
    assert pre._post_window_mode is False


def test_scheduler_loop_cache_ttl_matches_post_window_interval(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """Cache TTL is wired to the post_window_poll_interval_s — same cadence,
    same TTL. A tighter TTL would cause spurious refetches; a looser one would
    serve stale data past one full poll cycle."""
    cfg = _config(_booking())
    clock = make_clock()
    client = fake_client([])
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
        post_window_poll_interval_s=180,
    )
    assert loop._poll_cache.ttl_s == 180.0


def test_scheduler_loop_default_ttl_is_post_window_interval(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """Default loop construction uses DEFAULT_POST_WINDOW_POLL_INTERVAL_S."""
    cfg = _config(_booking())
    clock = make_clock()
    client = fake_client([])
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
    )
    assert loop._poll_cache.ttl_s == float(DEFAULT_POST_WINDOW_POLL_INTERVAL_S)


def test_scheduler_loop_pool_key_plumbed_from_booking(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """ResolvedBooking.pool_name → AttemptConfig.pool_key (for cache keying)."""
    booking = _booking(pool_name="evening")
    cfg = _config(booking)
    clock = make_clock()
    client = fake_client([])
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
    )
    from tennis_booking.scheduler.loop import ScheduledAttempt
    sa = ScheduledAttempt(
        booking=booking,
        slot_dt_local=datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY),
        window_open_utc=datetime(2026, 4, 29, 2, 0, tzinfo=UTC),
    )
    cfg_built = loop._build_attempt_config(sa)
    assert cfg_built.pool_key == "evening"


async def test_scheduler_loop_jitter_distributes_initial_polls(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """5 PollAttempts spawned in a tight loop → their initial jitter delays
    are not all zero; at least 4 of 5 land in different sleep buckets.

    This is what breaks the synchronised burst pattern Cloudflare flagged
    on 28.04.2026.
    """
    clock = make_clock()
    client = fake_client([])
    cfg = _config(_booking())
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(client),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
    )

    attempt_cfg = AttemptConfig(
        slot_dt_local=datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY),
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77001234567",
        profile_name="roman",
        pool_key="evening",
    )
    poll_data = PollConfigData(interval_s=120, start_offset_days=2)

    # Construct 5 PollAttempt instances and inspect what initial sleep each
    # would take. We use the production helper directly — no factory faff.
    class _RecClock:
        """Records the seconds passed to the most recent sleep call."""

        def __init__(self, base: FakeClock) -> None:
            self._base = base
            self.last: float | None = None

        def now_utc(self) -> datetime:
            return self._base.now_utc()

        def monotonic(self) -> float:
            return self._base.monotonic()

        async def sleep(self, seconds: float) -> None:
            self.last = seconds

    initial_sleeps: list[float] = []
    for _ in range(5):
        attempt = PollAttempt(
            attempt_cfg,
            poll_data,
            as_altegio_client(client),
            as_clock(clock),
            cache=loop._poll_cache,
            pool_key="evening",
        )
        rec = _RecClock(clock)
        # Direct invocation of the private helper — testing the jitter draw.
        attempt._clock = rec  # type: ignore[assignment]
        await attempt._initial_jitter_sleep()
        assert rec.last is not None, "initial jitter must register a sleep"
        initial_sleeps.append(rec.last)

    # All draws must be in [0, interval/2). With SystemRandom the chance of
    # any two of 5 draws being byte-identical is vanishingly small (uniform
    # double on [0, 60)).
    assert all(0.0 <= s < 60.0 for s in initial_sleeps), initial_sleeps
    # Spread check: at least 4 unique values out of 5.
    assert len(set(initial_sleeps)) >= 4, initial_sleeps


async def test_scheduler_loop_three_polls_same_date_one_fetch(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    ok_ntp_checker: FakeNTPChecker,
) -> None:
    """End-to-end: 3 PollAttempt instances (3 profiles, same pool, same date)
    sharing the loop's cache → only 1 search_timeslots call hits the network.

    This is the exact production scenario that motivated the change:
    21 polls (7 slots × 3 profiles) → 3 fetches (3 dates × 1 pool).
    For one date this collapses 3 → 1.
    """
    clock = make_clock()
    cfg = _config(_booking())
    loop = SchedulerLoop(
        config=cfg,
        altegio_client=as_altegio_client(fake_client([])),
        clock=as_clock(clock),
        ntp_threshold_ms=DEFAULT_NTP_THRESHOLD_MS,
        ntp_checker=ok_ntp_checker,  # type: ignore[arg-type]
    )

    # Build a fake AltegioClient that records search_timeslots calls.
    from tests.engine.test_poll_attempt import FakePollClient, _slot, as_client
    slot_dt = datetime(2026, 5, 1, 18, 0, tzinfo=ALMATY)
    initial = (slot_dt - timedelta(days=2)).astimezone(UTC)
    poll_clock = FakeClock(initial_utc=initial, initial_mono=1000.0)
    fake = FakePollClient(
        search_effects=[
            [_slot(slot_dt, is_bookable=True)],
        ],
        booking_effects=[
            BookingResponse(record_id=i, record_hash=f"h{i}") for i in range(1, 6)
        ],
    )

    attempt_cfg_a = AttemptConfig(
        slot_dt_local=slot_dt,
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77001234567",
        profile_name="roman",
        pool_key="evening",
    )
    attempt_cfg_b = AttemptConfig(
        slot_dt_local=slot_dt,
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Anya",
        phone="77001234568",
        profile_name="anya",
        pool_key="evening",
    )
    attempt_cfg_c = AttemptConfig(
        slot_dt_local=slot_dt,
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        fullname="Misha",
        phone="77001234569",
        profile_name="misha",
        pool_key="evening",
    )
    poll_data = PollConfigData(interval_s=60, start_offset_days=2)

    polls = [
        PollAttempt(
            cfg_,
            poll_data,
            as_client(fake),
            poll_clock,
            cache=loop._poll_cache,
            pool_key="evening",
        )
        for cfg_ in (attempt_cfg_a, attempt_cfg_b, attempt_cfg_c)
    ]
    results = await asyncio.gather(*(p.run() for p in polls))
    assert all(r.status == "won" for r in results)
    # The single load-shedding assertion: one search across three polls.
    assert len(fake.search_calls) == 1
    # Each profile fired its own booking POST (cache deduplicates search,
    # not booking — that's correct, each profile needs its own record).
    assert len(fake.booking_calls) == 3
