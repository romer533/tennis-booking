"""Shared poll-result cache.

Background — 28.04.2026 incident: post-window poll feature deployed for the
weekend kicked 21 simultaneous `search/timeslots` POST every 120s (3 dates ×
3 profiles × ~2-3 court groups per profile). Cloudflare bot detection trips
on exactly this pattern (synchronised bursts at fixed intervals).

Insight: `search_timeslots(date, staff_ids=pool)` returns the same response
for every poll instance sharing the same (date, pool). Profile / slot_time
/ booking_name are filters applied client-side AFTER the fetch — they do NOT
change what we ask Altegio for.

Goal: collapse N polls onto 1 fetch per (date, pool) per cycle. Every poll
goes through `PollResultCache.get_or_fetch(date, pool_key, fetch_fn)`. The
first poll fires the real HTTP request; concurrent + subsequent polls within
TTL receive the cached `list[TimeSlot]` and never touch the network.

Implementation notes:
  * key = (date, pool_key). `pool_key` is opaque from the cache's POV — caller
    builds it from pool_name (preferred) or sorted-court_ids fallback.
  * per-key asyncio.Lock dedupes concurrent first-fetches; double-checked
    locking inside the lock body covers waiters who arrived before the cache
    was filled by the winner.
  * On fetch_fn failure the cache is NOT poisoned: the previous (possibly
    stale) entry is left untouched so a transient error does not delete a
    still-useful result; if no entry exists, no entry is written. Either way
    the next call retries.
  * TTL is configurable; default = poll interval (caller passes it).
  * Light trim on insert: anything older than TRIM_AGE_S is evicted to keep
    memory bounded across days of running. Cache is small enough (handful of
    dates × pools) that O(n) sweep on insert is fine.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import structlog

from tennis_booking.altegio import TimeSlot
from tennis_booking.common.clock import Clock

__all__ = ["PollResultCache"]

_logger = structlog.get_logger(__name__)

# Anything older than this is dropped on the next insert. Keeps the dict from
# growing unbounded if the service runs for weeks: dates roll off after their
# slots have passed and we no longer poll them.
TRIM_AGE_S = 30 * 60.0


CacheKey = tuple[date, str]


@dataclass(frozen=True)
class _Entry:
    fetched_at_utc: datetime
    result: list[TimeSlot]


class PollResultCache:
    """In-process cache for `search_timeslots` results, keyed by (date, pool).

    Single instance is created by `SchedulerLoop` and shared across every
    `PollAttempt` it spawns — that is what coalesces the N-into-1 fetch.
    """

    def __init__(self, clock: Clock, *, ttl_s: float) -> None:
        if ttl_s <= 0.0:
            raise ValueError(f"ttl_s must be > 0, got {ttl_s}")
        self._clock = clock
        self._ttl_s = ttl_s
        self._entries: dict[CacheKey, _Entry] = {}
        self._locks: defaultdict[CacheKey, asyncio.Lock] = defaultdict(asyncio.Lock)

    @property
    def ttl_s(self) -> float:
        return self._ttl_s

    async def get_or_fetch(
        self,
        date_local: date,
        pool_key: str,
        fetch_fn: Callable[[], Awaitable[list[TimeSlot]]],
    ) -> list[TimeSlot]:
        key: CacheKey = (date_local, pool_key)
        now = self._clock.now_utc()

        # Fast path: fresh entry, no lock acquisition needed. Stale entries
        # fall through to the lock-protected refetch path.
        entry = self._entries.get(key)
        if entry is not None and not self._is_expired(entry, now):
            _logger.info(
                "poll_cache_hit",
                date=date_local.isoformat(),
                pool=pool_key,
                age_s=(now - entry.fetched_at_utc).total_seconds(),
            )
            return entry.result

        async with self._locks[key]:
            # Double-check: a concurrent waiter may have populated the entry
            # while we were blocked on the lock.
            now = self._clock.now_utc()
            entry = self._entries.get(key)
            if entry is not None and not self._is_expired(entry, now):
                _logger.info(
                    "poll_cache_hit",
                    date=date_local.isoformat(),
                    pool=pool_key,
                    age_s=(now - entry.fetched_at_utc).total_seconds(),
                    via="lock_double_check",
                )
                return entry.result

            if entry is not None:
                _logger.debug(
                    "poll_cache_expired",
                    date=date_local.isoformat(),
                    pool=pool_key,
                    age_s=(now - entry.fetched_at_utc).total_seconds(),
                )

            _logger.info(
                "poll_cache_miss",
                date=date_local.isoformat(),
                pool=pool_key,
            )
            # fetch_fn exceptions propagate to the caller; we deliberately do
            # NOT poison the cache (don't write a None entry, don't evict the
            # previous still-bookable entry). Next call by the same poll will
            # retry — that's the existing transport-error handling in poll.py.
            result = await fetch_fn()
            fetched_at = self._clock.now_utc()
            self._entries[key] = _Entry(fetched_at_utc=fetched_at, result=result)
            self._trim(fetched_at)
            return result

    def _is_expired(self, entry: _Entry, now_utc: datetime) -> bool:
        return (now_utc - entry.fetched_at_utc).total_seconds() >= self._ttl_s

    def _trim(self, now_utc: datetime) -> None:
        cutoff = now_utc - timedelta(seconds=TRIM_AGE_S)
        stale = [k for k, e in self._entries.items() if e.fetched_at_utc < cutoff]
        for k in stale:
            self._entries.pop(k, None)
            # Lock for an evicted key may still be held by something racing in
            # `get_or_fetch`; defaultdict will just re-create on next access.
            # Drop only if no waiters → safe to leave; defaultdict re-create
            # is cheap. We do not pop from `_locks` here to avoid orphaning a
            # held lock; re-population is fine because evictions are rare.

    # --- test / introspection helpers ----------------------------------

    def _peek(self, key: CacheKey) -> _Entry | None:
        """Internal: read without taking the lock. Tests only."""
        return self._entries.get(key)

    def _size(self) -> int:
        return len(self._entries)
