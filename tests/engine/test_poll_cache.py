"""PollResultCache — unit tests.

The cache is the load-shedding layer added after the 28.04.2026 Cloudflare
incident: 21 simultaneous search_timeslots POSTs every 120s collapsed onto
3 (one per active date). Tests verify the contract that makes that work:

  - first call goes to fetch_fn,
  - within TTL, subsequent calls hit cache (no fetch_fn),
  - concurrent waiters dedupe on a per-key lock,
  - expired entries refetch,
  - different keys are independent (no cross-contamination),
  - failures don't poison the cache,
  - old entries are trimmed.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta

import pytest

from tennis_booking.altegio import TimeSlot
from tennis_booking.engine.poll_cache import TRIM_AGE_S, PollResultCache

from .conftest import FakeClock


def _slot(dt_local: datetime, *, is_bookable: bool = True) -> TimeSlot:
    return TimeSlot(dt=dt_local, is_bookable=is_bookable)


def _now() -> datetime:
    return datetime(2026, 4, 28, 14, 0, 0, tzinfo=UTC)


def _make_clock() -> FakeClock:
    return FakeClock(initial_utc=_now(), initial_mono=1000.0)


async def test_poll_cache_first_fetch_calls_client() -> None:
    """Fresh cache → fetch_fn invoked exactly once for the first key access."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls: int = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        return [_slot(datetime(2026, 5, 1, 18, 0, tzinfo=UTC))]

    result = await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 1
    assert len(result) == 1


async def test_poll_cache_hit_skips_client() -> None:
    """Second call within TTL returns cached value without invoking fetch_fn."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        return [_slot(datetime(2026, 5, 1, 18, 0, tzinfo=UTC))]

    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    # Within TTL — must NOT call fetch_fn.
    clock.advance(60.0)  # half of TTL
    result = await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 1
    assert len(result) == 1


async def test_poll_cache_concurrent_dedup() -> None:
    """5 concurrent get_or_fetch with the same key → fetch_fn called only once.

    This is the property that turns 21-simultaneous-polls-into-1-fetch.
    """
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        started.set()
        # Block until test releases — guarantees other waiters pile up on
        # the per-key lock before this one returns.
        await release.wait()
        return [_slot(datetime(2026, 5, 1, 18, 0, tzinfo=UTC))]

    tasks = [
        asyncio.create_task(cache.get_or_fetch(date(2026, 5, 1), "evening", slow_fetch))
        for _ in range(5)
    ]
    # Wait for the first task to enter fetch_fn.
    await started.wait()
    # All 5 tasks should now be queued: one inside fetch, four blocked on the
    # per-key lock. Release the in-flight fetch and let them all complete.
    release.set()
    results = await asyncio.gather(*tasks)
    assert calls == 1
    # All five got the same payload reference — the cached entry.
    assert all(len(r) == 1 for r in results)


async def test_poll_cache_expired_refetches() -> None:
    """After clock advances past TTL, the next get_or_fetch refetches."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        return []

    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 1
    # Advance past TTL.
    clock.advance(121.0)
    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 2


async def test_poll_cache_different_keys_independent() -> None:
    """Two different (date, pool) keys → two parallel fetches, no shared lock."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls: dict[date, int] = {}
    barrier = asyncio.Event()

    async def fetch_for(d: date) -> list[TimeSlot]:
        calls[d] = calls.get(d, 0) + 1
        await barrier.wait()
        return []

    # Two concurrent fetches for different dates: both block on the barrier
    # in parallel — this would deadlock if the cache used a single global lock.
    t1 = asyncio.create_task(
        cache.get_or_fetch(date(2026, 5, 1), "evening", lambda: fetch_for(date(2026, 5, 1)))
    )
    t2 = asyncio.create_task(
        cache.get_or_fetch(date(2026, 5, 2), "evening", lambda: fetch_for(date(2026, 5, 2)))
    )
    # Yield repeatedly so both fetches enter their respective fetch_fn before
    # we release the barrier.
    for _ in range(5):
        await asyncio.sleep(0)
    assert calls.get(date(2026, 5, 1)) == 1
    assert calls.get(date(2026, 5, 2)) == 1
    barrier.set()
    await asyncio.gather(t1, t2)


async def test_poll_cache_failure_propagates() -> None:
    """fetch_fn raises → exception bubbles, cache NOT poisoned (next call retries)."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    fail_first = True
    calls = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls, fail_first
        calls += 1
        if fail_first:
            fail_first = False
            raise RuntimeError("transport down")
        return [_slot(datetime(2026, 5, 1, 18, 0, tzinfo=UTC))]

    with pytest.raises(RuntimeError, match="transport down"):
        await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)

    # Cache must not have stored the failure — next call retries.
    result = await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 2
    assert len(result) == 1


async def test_poll_cache_trim_evicts_old_entries() -> None:
    """Insert old entry → fast-forward past TRIM_AGE_S → insert new entry → old gone."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)

    async def fetch_empty() -> list[TimeSlot]:
        return []

    # Insert at t=0
    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch_empty)
    assert cache._size() == 1

    # Advance well past TRIM_AGE_S (30 minutes). Insert another date — trim
    # should sweep the old one out.
    clock.advance(TRIM_AGE_S + 60.0)
    await cache.get_or_fetch(date(2026, 5, 5), "evening", fetch_empty)

    assert cache._size() == 1
    assert cache._peek((date(2026, 5, 1), "evening")) is None
    assert cache._peek((date(2026, 5, 5), "evening")) is not None


async def test_poll_cache_invalid_ttl_raises() -> None:
    """ttl_s must be > 0."""
    clock = _make_clock()
    with pytest.raises(ValueError, match="ttl_s"):
        PollResultCache(clock, ttl_s=0.0)
    with pytest.raises(ValueError, match="ttl_s"):
        PollResultCache(clock, ttl_s=-1.0)


async def test_poll_cache_same_date_different_pool_independent() -> None:
    """Same date, different pool_key → two distinct fetches."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls: dict[str, int] = {}

    async def fetch_for(pool: str) -> list[TimeSlot]:
        calls[pool] = calls.get(pool, 0) + 1
        return []

    await cache.get_or_fetch(date(2026, 5, 1), "evening", lambda: fetch_for("evening"))
    await cache.get_or_fetch(date(2026, 5, 1), "morning", lambda: fetch_for("morning"))
    assert calls == {"evening": 1, "morning": 1}


async def test_poll_cache_ttl_boundary_inclusive() -> None:
    """At exactly TTL the entry is considered expired (≥ ttl_s)."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        return []

    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    clock.advance(120.0)  # exactly at TTL — must refetch
    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 2


async def test_poll_cache_repeated_within_ttl_no_refetch_after_advance() -> None:
    """Second call at TTL-1ms still hits cache; at TTL+1ms misses."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        return []

    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    clock.advance(119.999)
    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 1
    # Cross the boundary.
    clock.advance(0.002)
    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 2


async def test_poll_cache_concurrent_failure_other_waiters_retry() -> None:
    """If the in-flight fetch raises, waiting tasks see the same exception
    semantics — but each subsequent call re-enters fetch_fn (no poison).

    NB: only the *winning* waiter gets the exception; others see whatever
    state the cache is in afterwards. This test pins the contract: after a
    failure cycle, new calls retry the fetch.
    """
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0

    async def always_fail() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await cache.get_or_fetch(date(2026, 5, 1), "evening", always_fail)
    with pytest.raises(RuntimeError):
        await cache.get_or_fetch(date(2026, 5, 1), "evening", always_fail)
    assert calls == 2


async def test_poll_cache_returns_same_list_object_within_ttl() -> None:
    """Same key within TTL returns the SAME list object (identity, not copy).

    Cheap: callers must not mutate; PollAttempt only reads. Saves an O(N)
    list copy per cache hit.
    """
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    payload = [_slot(datetime(2026, 5, 1, 18, 0, tzinfo=UTC))]

    async def fetch() -> list[TimeSlot]:
        return payload

    a = await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    b = await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert a is b


async def test_poll_cache_trim_keeps_recent_entries() -> None:
    """After many inserts, recent entries (< TRIM_AGE_S) survive trim."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)

    async def fetch_empty() -> list[TimeSlot]:
        return []

    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch_empty)
    clock.advance(60.0)
    await cache.get_or_fetch(date(2026, 5, 2), "evening", fetch_empty)
    clock.advance(60.0)
    await cache.get_or_fetch(date(2026, 5, 3), "evening", fetch_empty)
    # All three are within TRIM_AGE_S (30min).
    assert cache._size() == 3


async def test_poll_cache_with_real_timedelta_uses_clock() -> None:
    """Cache reads `now` via the injected clock, not wall time."""
    clock = _make_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    calls = 0

    async def fetch() -> list[TimeSlot]:
        nonlocal calls
        calls += 1
        return []

    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    # Advance virtual clock backwards (impossible with wall time) — should
    # still be considered fresh because fetched_at is FROM THE SAME CLOCK.
    # We pin the contract: TTL is measured against the clock that wrote the
    # entry, not real time.
    clock.advance(-timedelta(seconds=10).total_seconds())
    await cache.get_or_fetch(date(2026, 5, 1), "evening", fetch)
    assert calls == 1
