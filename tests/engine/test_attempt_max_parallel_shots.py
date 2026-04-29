"""Engine tests for max_parallel_shots cap.

Defensive measure against Cloudflare's per-IP rate-rule (~30 RPS): cap fan-out
to N random court_ids from the pool instead of firing on all of them. Backward
compat is preserved when max_parallel_shots is None (existing behavior).
"""
from __future__ import annotations

import random
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest

from tennis_booking.altegio import BookingResponse
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    SLOT,
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111) -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=f"h{record_id}")


# ---------- AttemptConfig validation ----------------------------------------


class TestAttemptConfigMaxParallelShotsValidation:
    def test_max_parallel_shots_zero_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_parallel_shots"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_ids=(1, 2, 3),
                service_id=1,
                fullname="R",
                phone="77000",
                profile_name="roman",
                max_parallel_shots=0,
            )

    def test_max_parallel_shots_negative_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_parallel_shots"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_ids=(1, 2, 3),
                service_id=1,
                fullname="R",
                phone="77000",
                profile_name="roman",
                max_parallel_shots=-1,
            )

    def test_max_parallel_shots_one_ok(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1, 2, 3),
            service_id=1,
            fullname="R",
            phone="77000",
            profile_name="roman",
            max_parallel_shots=1,
        )
        assert cfg.max_parallel_shots == 1
        assert cfg.effective_shots == 1

    def test_max_parallel_shots_default_none(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1, 2, 3),
            service_id=1,
            fullname="R",
            phone="77000",
            profile_name="roman",
        )
        assert cfg.max_parallel_shots is None
        assert cfg.effective_shots == 3


# ---------- effective_shots respects cap ------------------------------------


class TestEffectiveShotsCapped:
    def test_pool_capped_below_size(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1, 2, 3, 4, 5, 6, 7),
            service_id=1,
            fullname="R",
            phone="77000",
            profile_name="roman",
            max_parallel_shots=3,
        )
        assert cfg.effective_shots == 3

    def test_pool_capped_above_size_uses_pool_size(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1, 2, 3),
            service_id=1,
            fullname="R",
            phone="77000",
            profile_name="roman",
            max_parallel_shots=10,
        )
        assert cfg.effective_shots == 3


# ---------- runtime fan-out behaviour ---------------------------------------


class TestMaxParallelShotsRuntime:
    async def test_attempt_with_max_parallel_shots_3_picks_3_court_ids(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # Pool of 7 — only 3 should fire.
        client = fake_client([_booking(1), _booking(2), _booking(3)])
        cfg = attempt_config(
            court_ids=(101, 102, 103, 104, 105, 106, 107),
            max_parallel_shots=3,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 3
        assert len(client.create_booking_calls) == 3
        called_courts = [c["staff_id"] for c in client.create_booking_calls]
        # Each chosen court must be from the pool, no duplicates (sample without replacement).
        assert len(set(called_courts)) == 3
        for cid in called_courts:
            assert cid in (101, 102, 103, 104, 105, 106, 107)

    async def test_attempt_with_max_parallel_shots_random_subset(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Two successive attempts on the same pool with different RNG seeds
        produce different subsets (probabilistic — pinned with explicit seeds
        so the assertion is deterministic).
        """
        pool = (101, 102, 103, 104, 105, 106, 107)

        # Seed 1: deterministic pick.
        clock1 = make_clock()
        client1 = fake_client([_booking(1), _booking(2), _booking(3)])
        cfg = attempt_config(court_ids=pool, max_parallel_shots=3)
        attempt1 = BookingAttempt(
            cfg,
            as_altegio_client(client1),
            as_clock(clock1),
            rng=random.Random(1),
        )
        await attempt1.run(window_open)
        subset1 = sorted(c["staff_id"] for c in client1.create_booking_calls)

        # Seed 2: different deterministic pick.
        clock2 = make_clock()
        client2 = fake_client([_booking(1), _booking(2), _booking(3)])
        attempt2 = BookingAttempt(
            cfg,
            as_altegio_client(client2),
            as_clock(clock2),
            rng=random.Random(2),
        )
        await attempt2.run(window_open)
        subset2 = sorted(c["staff_id"] for c in client2.create_booking_calls)

        # The two subsets must not be identical: with C(7,3)=35 possibilities,
        # seeds 1 vs 2 yield different choices in cpython's random.
        assert subset1 != subset2
        assert len(subset1) == 3
        assert len(subset2) == 3

    async def test_attempt_with_max_parallel_shots_clamped_to_pool_size(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2), _booking(3)])
        # Pool 3, cap 10 → fire 3 (no error, no spillover).
        cfg = attempt_config(court_ids=(101, 102, 103), max_parallel_shots=10)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 3
        called_courts = sorted(c["staff_id"] for c in client.create_booking_calls)
        assert called_courts == [101, 102, 103]

    async def test_attempt_without_max_parallel_shots_fans_out_all(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Backward compat: max_parallel_shots=None → fire on entire pool."""
        clock = make_clock()
        client = fake_client([_booking(i) for i in range(7)])
        cfg = attempt_config(
            court_ids=(101, 102, 103, 104, 105, 106, 107),
            max_parallel_shots=None,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 7
        called_courts = sorted(c["staff_id"] for c in client.create_booking_calls)
        assert called_courts == [101, 102, 103, 104, 105, 106, 107]

    async def test_won_court_id_from_active_subset_only(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Persisted win records the actual court_id we fired on, not a
        court that wasn't in the active subset.
        """
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2), _booking(3)])
        pool = (101, 102, 103, 104, 105, 106, 107)
        cfg = attempt_config(court_ids=pool, max_parallel_shots=3)

        attempt = BookingAttempt(
            cfg,
            as_altegio_client(client),
            as_clock(clock),
            rng=random.Random(42),
        )
        result = await attempt.run(window_open)

        assert result.status == "won"
        # The won_court_id is internal but observable via `create_booking_calls`:
        # the first successful response maps to the first idx → first active
        # court. We just check it's from the pool.
        first_called = client.create_booking_calls[0]["staff_id"]
        assert first_called in pool

    async def test_single_court_unaffected_by_max_parallel_shots(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Legacy single-court mode: parallel_shots controls duplication onto
        the lone court; max_parallel_shots is irrelevant (pool size==1).
        """
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2)])
        cfg = attempt_config(
            court_ids=(555,),
            parallel_shots=2,
            max_parallel_shots=3,  # ignored: single-court
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 2
        assert all(c["staff_id"] == 555 for c in client.create_booking_calls)

    async def test_max_parallel_shots_one_fires_single_shot(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1)])
        cfg = attempt_config(
            court_ids=(101, 102, 103, 104, 105, 106, 107),
            max_parallel_shots=1,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 1
        assert len(client.create_booking_calls) == 1


# ---------- BookingRule / ResolvedBooking schema ----------------------------


class TestSchemaMaxParallelShots:
    def test_booking_rule_default_none(self) -> None:
        from tennis_booking.config.schema import BookingRule

        rule = BookingRule(
            name="x",
            weekday="friday",  # type: ignore[arg-type]
            slot_local_time="18:00",  # type: ignore[arg-type]
            duration_minutes=60,
            court_id=1,
            service_id=1,
            profile="roman",
        )
        assert rule.max_parallel_shots is None

    def test_booking_rule_max_parallel_shots_set(self) -> None:
        from tennis_booking.config.schema import BookingRule

        rule = BookingRule(
            name="x",
            weekday="friday",  # type: ignore[arg-type]
            slot_local_time="18:00",  # type: ignore[arg-type]
            duration_minutes=60,
            court_id=1,
            service_id=1,
            profile="roman",
            max_parallel_shots=3,
        )
        assert rule.max_parallel_shots == 3

    def test_booking_rule_max_parallel_shots_zero_rejected(self) -> None:
        from pydantic import ValidationError

        from tennis_booking.config.schema import BookingRule

        with pytest.raises(ValidationError):
            BookingRule(
                name="x",
                weekday="friday",  # type: ignore[arg-type]
                slot_local_time="18:00",  # type: ignore[arg-type]
                duration_minutes=60,
                court_id=1,
                service_id=1,
                profile="roman",
                max_parallel_shots=0,
            )


__all__: list[Any] = []
