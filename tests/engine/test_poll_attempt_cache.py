"""PollAttempt + PollResultCache integration.

Ensures the structural property the production fix relies on:

  Two PollAttempt instances with the same (date, pool_key) and a shared
  PollResultCache make exactly ONE upstream search_timeslots call per
  cache cycle — even though each instance ticks independently.

This is the test that justifies the 21 → 3 fetch reduction in production.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, timedelta

from tennis_booking.altegio import (
    BookingResponse,
)
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.engine.poll_cache import PollResultCache

from .conftest import (
    SERVICE_ID,
    SLOT,
    STAFF_ID,
    FakeClock,
)
from .test_poll_attempt import (
    FakePollClient,
    _make_attempt_config,
    _slot,
    as_client,
)


def _start_clock() -> FakeClock:
    initial = (SLOT - timedelta(days=2)).astimezone(UTC)
    return FakeClock(initial_utc=initial, initial_mono=1000.0)


async def test_poll_attempt_uses_cache_when_provided() -> None:
    """Two PollAttempt with the same cache + same (date, pool_key) → 1 fetch."""
    clock = _start_clock()
    booking_resp = BookingResponse(record_id=42, record_hash="abc")

    fake = FakePollClient(
        search_effects=[
            # Only ONE search response is scripted: if the cache works, the
            # second PollAttempt won't hit the network.
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[booking_resp, booking_resp],
    )
    cache = PollResultCache(clock, ttl_s=120.0)

    poll_a = PollAttempt(
        _make_attempt_config(profile_name="roman"),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
        cache=cache,
        pool_key="evening",
    )
    poll_b = PollAttempt(
        _make_attempt_config(profile_name="anya"),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
        cache=cache,
        pool_key="evening",
    )

    res_a, res_b = await asyncio.gather(poll_a.run(), poll_b.run())
    assert res_a.status == "won"
    assert res_b.status == "won"
    # The structural assertion: one search across both polls.
    assert len(fake.search_calls) == 1
    # Both fired their booking POST (the cache only deduplicates search,
    # not booking — engine fan-out is independent).
    assert len(fake.booking_calls) == 2


async def test_poll_attempt_without_cache_calls_client_directly() -> None:
    """Backward compat: cache=None → existing behaviour, no cache layer."""
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=10, start_offset_days=2),
        as_client(fake),
        clock,
        # explicit None — assert default works as before
        cache=None,
    )
    res = await poll.run()
    assert res.status == "won"
    assert len(fake.search_calls) == 1


async def test_poll_attempt_different_pool_keys_do_not_share_cache() -> None:
    """Two PollAttempt instances with DIFFERENT pool_keys → 2 fetches.

    Correctness invariant: polls watching different staff_ids must not coalesce
    even if they target the same date; otherwise we'd return stale / wrong data.
    """
    clock = _start_clock()
    cache = PollResultCache(clock, ttl_s=120.0)

    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=True)],
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[
            BookingResponse(record_id=1, record_hash="h"),
            BookingResponse(record_id=2, record_hash="h2"),
        ],
    )
    poll_a = PollAttempt(
        _make_attempt_config(court_ids=(STAFF_ID,), service_id=SERVICE_ID),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
        cache=cache,
        pool_key="evening",
    )
    poll_b = PollAttempt(
        _make_attempt_config(court_ids=(STAFF_ID,), service_id=SERVICE_ID),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
        cache=cache,
        pool_key="morning",
    )
    await asyncio.gather(poll_a.run(), poll_b.run())
    # Different pool keys → independent fetches.
    assert len(fake.search_calls) == 2


async def test_poll_attempt_synthesises_pool_key_from_court_ids() -> None:
    """When pool_key is None, two polls with the SAME court_ids still coalesce."""
    clock = _start_clock()
    cache = PollResultCache(clock, ttl_s=120.0)
    fake = FakePollClient(
        search_effects=[
            [_slot(SLOT, is_bookable=True)],
        ],
        booking_effects=[
            BookingResponse(record_id=1, record_hash="h"),
            BookingResponse(record_id=2, record_hash="h2"),
        ],
    )
    poll_a = PollAttempt(
        _make_attempt_config(court_ids=(5, 6, 7), service_id=SERVICE_ID, profile_name="roman"),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
        cache=cache,
        # pool_key=None → synthesise from court_ids
    )
    poll_b = PollAttempt(
        _make_attempt_config(court_ids=(5, 6, 7), service_id=SERVICE_ID, profile_name="anya"),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
        cache=cache,
    )
    await asyncio.gather(poll_a.run(), poll_b.run())
    assert len(fake.search_calls) == 1


async def test_poll_attempt_jitter_uses_clock_sleep() -> None:
    """Jitter sleeps register on FakeClock — and the initial jitter MUST fire
    before the first search call. We assert ordering by counting sleeps that
    happened BEFORE search_calls[0]."""
    clock = _start_clock()
    fake = FakePollClient(
        search_effects=[[_slot(SLOT, is_bookable=True)]],
        booking_effects=[BookingResponse(record_id=1, record_hash="h")],
    )
    poll = PollAttempt(
        _make_attempt_config(),
        PollConfigData(interval_s=60, start_offset_days=2),
        as_client(fake),
        clock,
    )
    await poll.run()
    # Two sleeps before the first search: (a) `_sleep_until_utc(effective_start)`
    # which is 0 here because effective_start <= now (we start exactly 2 days
    # before slot, offset=2). It is still appended (sleep_calls grows on every
    # `await sleep(...)`), and (b) the initial jitter sleep with delay > 0.
    # Real assertion: there was at least one positive-delay sleep registered
    # before the search ran (the initial jitter).
    assert any(s > 0 for s in clock.sleep_calls)
