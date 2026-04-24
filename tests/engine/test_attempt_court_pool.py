"""Engine tests for court_pool fan-out: shots dispatched to different courts,
first-success cancels rest, duplicates collected, dry-run path."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from tennis_booking.altegio import (
    AltegioBusinessError,
    BookingResponse,
)
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


def _business(code: str, http_status: int = 422) -> AltegioBusinessError:
    return AltegioBusinessError(code=code, message=f"test-{code}", http_status=http_status)


# ---------- AttemptConfig.court_ids validation ------------------------------


class TestAttemptConfigCourtIdsValidation:
    def test_empty_court_ids_rejected(self) -> None:
        with pytest.raises(ValueError, match="court_ids"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_ids=(),
                service_id=1,
                fullname="R",
                phone="77000",
            )

    def test_zero_court_id_in_tuple_rejected(self) -> None:
        with pytest.raises(ValueError, match="court_ids"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_ids=(1, 0, 2),
                service_id=1,
                fullname="R",
                phone="77000",
            )

    def test_duplicate_court_ids_rejected(self) -> None:
        with pytest.raises(ValueError, match="unique"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_ids=(1, 2, 1),
                service_id=1,
                fullname="R",
                phone="77000",
            )

    def test_list_instead_of_tuple_rejected(self) -> None:
        with pytest.raises(ValueError, match="tuple"):
            AttemptConfig(
                slot_dt_local=SLOT,
                court_ids=[1, 2],  # type: ignore[arg-type]
                service_id=1,
                fullname="R",
                phone="77000",
            )

    def test_naive_datetime_still_rejected_with_pool(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            AttemptConfig(
                slot_dt_local=datetime(2026, 4, 26, 23, 0),
                court_ids=(1, 2),
                service_id=1,
                fullname="R",
                phone="77000",
            )

    def test_utc_datetime_still_rejected_with_pool(self) -> None:
        with pytest.raises(ValueError, match="Asia/Almaty"):
            AttemptConfig(
                slot_dt_local=datetime(2026, 4, 26, 23, 0, tzinfo=UTC),
                court_ids=(1, 2),
                service_id=1,
                fullname="R",
                phone="77000",
            )


# ---------- effective_shots computation -------------------------------------


class TestEffectiveShots:
    def test_pool_shots_equals_court_count(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1, 2, 3, 4, 5),
            service_id=7849893,
            fullname="R",
            phone="77000",
            parallel_shots=2,  # ignored for pool
        )
        assert cfg.effective_shots == 5

    def test_legacy_single_court_uses_parallel_shots(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=7849893,
            fullname="R",
            phone="77000",
            parallel_shots=2,
        )
        assert cfg.effective_shots == 2

    def test_legacy_parallel_shots_1(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=7849893,
            fullname="R",
            phone="77000",
            parallel_shots=1,
        )
        assert cfg.effective_shots == 1


# ---------- pool fan-out behaviour ------------------------------------------


class TestPoolFanOut:
    async def test_three_court_pool_spawns_three_shots(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2), _booking(3)])
        cfg = attempt_config(court_ids=(101, 102, 103), parallel_shots=99)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 3
        assert len(client.create_booking_calls) == 3
        court_ids_called = sorted(c["staff_id"] for c in client.create_booking_calls)
        assert court_ids_called == [101, 102, 103]

    async def test_pool_uses_one_shot_per_court(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2)])
        cfg = attempt_config(court_ids=(50, 60), parallel_shots=99)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        await attempt.run(window_open)

        called_courts = {c["staff_id"] for c in client.create_booking_calls}
        assert called_courts == {50, 60}

    async def test_pool_first_success_cancels_remaining(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        async def fast() -> BookingResponse:
            await asyncio.sleep(0)
            return _booking(99)

        async def hang() -> BookingResponse:
            await asyncio.sleep(3600)
            return _booking(100)

        client = fake_client([fast, hang, hang])
        cfg = attempt_config(court_ids=(1, 2, 3))

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 99

    async def test_pool_all_slot_taken_lost(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from tennis_booking.engine import attempt as attempt_module
        from tennis_booking.engine import codes as codes_module

        monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
        monkeypatch.setattr(attempt_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
        clock = make_clock()
        client = fake_client(
            [_business("slot_busy"), _business("slot_busy"), _business("slot_busy")]
        )
        cfg = attempt_config(court_ids=(1, 2, 3))

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "lost"
        assert result.business_code == "slot_busy"
        assert result.shots_fired == 3

    async def test_pool_one_wins_others_slot_taken_priority_win(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from tennis_booking.engine import attempt as attempt_module
        from tennis_booking.engine import codes as codes_module

        monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
        monkeypatch.setattr(attempt_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
        clock = make_clock()

        async def slow_win() -> BookingResponse:
            for _ in range(5):
                await asyncio.sleep(0)
            return _booking(777)

        async def slow_taken() -> BookingResponse:
            for _ in range(5):
                await asyncio.sleep(0)
            raise _business("slot_busy")

        client = fake_client([slow_win, slow_taken, slow_taken])
        cfg = attempt_config(court_ids=(1, 2, 3))

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 777

    async def test_pool_collects_duplicates_from_late_winners(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        async def shot_n(n: int) -> Callable[[], Any]:
            async def _fn() -> BookingResponse:
                for _ in range(10):
                    await asyncio.sleep(0)
                return _booking(n)
            return _fn

        client = fake_client(
            [
                await shot_n(1),
                await shot_n(2),
                await shot_n(3),
            ]
        )
        cfg = attempt_config(court_ids=(10, 20, 30))
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        # Duplicates may be 0..2 depending on which task finishes first; just require structure.
        assert result.booking is not None
        assert len(result.duplicates) <= 2

    async def test_pool_each_shot_gets_distinct_court(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2), _booking(3), _booking(4)])
        cfg = attempt_config(court_ids=(11, 22, 33, 44), parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        await attempt.run(window_open)

        called = sorted(c["staff_id"] for c in client.create_booking_calls)
        assert called == [11, 22, 33, 44]


# ---------- legacy single-court behaviour preserved -------------------------


class TestLegacySingleCourtPreserved:
    async def test_single_court_uses_parallel_shots(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1), _booking(2)])
        cfg = attempt_config(court_ids=(555,), parallel_shots=2)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 2
        # Both shots go to same court 555.
        assert all(c["staff_id"] == 555 for c in client.create_booking_calls)

    async def test_single_court_parallel_shots_1(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(1)])
        cfg = attempt_config(court_ids=(7,), parallel_shots=1)

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.shots_fired == 1
        assert client.create_booking_calls[0]["staff_id"] == 7


# ---------- dry-run with pool -----------------------------------------------


class TestDryRunPool:
    async def test_dry_run_pool_won(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client(
            [
                BookingResponse(record_id=0, record_hash="dry-run"),
                BookingResponse(record_id=0, record_hash="dry-run"),
                BookingResponse(record_id=0, record_hash="dry-run"),
            ],
            dry_run=True,
        )
        cfg = attempt_config(court_ids=(1, 2, 3))

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_hash == "dry-run"


# ---------- structlog binding inspection ------------------------------------


class TestLogBinding:
    def test_small_pool_uses_court_ids_field(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        cfg = attempt_config(court_ids=(1, 2, 3))
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        bindings = attempt._log._context  # type: ignore[attr-defined]
        assert "court_ids" in bindings
        assert bindings["court_ids"] == (1, 2, 3)
        assert "court_id_primary" not in bindings

    def test_large_pool_uses_primary_and_count(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        cfg = attempt_config(court_ids=(11, 22, 33, 44, 55, 66, 77, 88))
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        bindings = attempt._log._context  # type: ignore[attr-defined]
        assert bindings.get("court_id_primary") == 11
        assert bindings.get("court_count") == 8
        assert "court_ids" not in bindings


# ---------- pool with cancel race -------------------------------------------


class TestPoolCancelRace:
    async def test_pool_external_cancel_cleans_up(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()

        async def hang() -> BookingResponse:
            await asyncio.sleep(3600)
            return _booking(1)

        client = fake_client([hang, hang, hang, hang])
        cfg = attempt_config(court_ids=(1, 2, 3, 4))

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(attempt.run(window_open), timeout=0.5)
