"""BookingAttempt — persistence hook on win path.

Verifies:
  - store.append called with correct BookedSlot on win
  - winning court_id correctly identified for legacy single-court
  - winning court_id correctly identified for court_pool (fan-out)
  - persistence failure does NOT bubble into result (engine swallows)
  - no-store mode (store=None) still works (backwards compat)
  - lost / timeout / business-error paths do NOT call store
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest

from tennis_booking.altegio import AltegioBusinessError, BookingResponse
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt
from tennis_booking.persistence.models import BookedSlot
from tennis_booking.persistence.store import MemoryBookingStore

from .conftest import (
    SLOT,
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


def _booking(record_id: int = 111, record_hash: str = "hash-a") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


def _business(code: str, http_status: int = 422) -> AltegioBusinessError:
    return AltegioBusinessError(code=code, message=f"test-{code}", http_status=http_status)


class _RaisingStore:
    """Store whose .append always raises — to exercise engine's swallow logic."""

    async def append(self, slot: BookedSlot) -> None:
        raise RuntimeError("disk full")

    async def find(
        self,
        slot_dt_local: datetime,
        court_ids: list[int],
        service_id: int,
        profile_name: str,
    ) -> BookedSlot | None:
        return None

    async def all_for_profile(self, profile_name: str) -> list[BookedSlot]:
        return []


@pytest.mark.asyncio
async def test_win_appends_booked_slot(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    store = MemoryBookingStore()
    client = fake_client([_booking(record_id=42, record_hash="h42")])
    clock = make_clock()
    cfg = attempt_config(parallel_shots=1, prearm_lead_s=10.0)

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(window_open)

    assert result.status == "won"
    assert result.booking is not None
    persisted = await store.all_for_profile(cfg.profile_name)
    assert len(persisted) == 1
    rec = persisted[0]
    assert rec.record_id == 42
    assert rec.record_hash == "h42"
    assert rec.court_id == cfg.court_ids[0]
    assert rec.service_id == cfg.service_id
    assert rec.profile_name == cfg.profile_name
    assert rec.phase == "window"
    assert rec.slot_dt_local == cfg.slot_dt_local


@pytest.mark.asyncio
async def test_no_store_does_not_break_win(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    client = fake_client([_booking()])
    clock = make_clock()
    cfg = attempt_config(parallel_shots=1, prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)
    assert result.status == "won"


@pytest.mark.asyncio
async def test_persistence_failure_is_swallowed(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    client = fake_client([_booking(record_id=99)])
    clock = make_clock()
    cfg = attempt_config(parallel_shots=1, prearm_lead_s=10.0)
    attempt = BookingAttempt(
        cfg,
        as_altegio_client(client),
        as_clock(clock),
        store=_RaisingStore(),  # type: ignore[arg-type] — duck typed BookingStore
    )
    result = await attempt.run(window_open)
    assert result.status == "won"
    assert result.booking is not None
    assert result.booking.record_id == 99


@pytest.mark.asyncio
async def test_pool_winning_court_id_recorded(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    """Pool of 3 courts: one fast win, two hangers. Engine records first response.
    Since FIFO consumes in launch order, shot at idx=0 wins → court_ids[0]."""
    store = MemoryBookingStore()

    async def fast() -> BookingResponse:
        await asyncio.sleep(0)
        return _booking(record_id=222, record_hash="h222")

    async def hang() -> BookingResponse:
        await asyncio.sleep(3600)
        return _booking(record_id=999, record_hash="h999")

    client = fake_client([fast, hang, hang])
    clock = make_clock()
    cfg = attempt_config(court_ids=(11, 22, 33), prearm_lead_s=10.0)

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(window_open)
    assert result.status == "won"
    persisted = await store.all_for_profile(cfg.profile_name)
    assert len(persisted) == 1
    assert persisted[0].record_id == 222
    assert persisted[0].court_id == 11  # court_ids[0] — first idx wins


@pytest.mark.asyncio
async def test_pool_second_court_wins_recorded(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When shot 0 raises slot_taken-classified business error and shot 1 wins,
    the persisted court_id reflects court_ids[1]."""
    from tennis_booking.engine import attempt as attempt_module
    from tennis_booking.engine import codes as codes_module

    monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
    monkeypatch.setattr(attempt_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))

    store = MemoryBookingStore()

    async def slow_win() -> BookingResponse:
        for _ in range(5):
            await asyncio.sleep(0)
        return _booking(record_id=333, record_hash="h333")

    async def slow_lost() -> BookingResponse:
        for _ in range(5):
            await asyncio.sleep(0)
        raise _business("slot_busy")

    # Layout: idx=0 → slow_lost, idx=1 → slow_win, idx=2 → slow_lost.
    # Engine classifies all `done` tasks and applies priority — win beats lost.
    client = fake_client([slow_lost, slow_win, slow_lost])
    clock = make_clock()
    cfg = attempt_config(court_ids=(11, 22, 33), prearm_lead_s=10.0)

    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(window_open)
    assert result.status == "won"
    persisted = await store.all_for_profile(cfg.profile_name)
    assert len(persisted) == 1
    assert persisted[0].record_id == 333
    assert persisted[0].court_id == 22  # court_ids[1]


@pytest.mark.asyncio
async def test_lost_does_not_persist(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tennis_booking.engine import attempt as attempt_module
    from tennis_booking.engine import codes as codes_module

    monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))
    monkeypatch.setattr(attempt_module, "SLOT_TAKEN_CODES", frozenset({"slot_busy"}))

    store = MemoryBookingStore()
    client = fake_client(
        [
            _business("slot_busy"),
            _business("slot_busy"),
        ]
    )
    clock = make_clock()
    cfg = attempt_config(prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(window_open)
    assert result.status == "lost"
    assert await store.all_for_profile(cfg.profile_name) == []


@pytest.mark.asyncio
async def test_config_error_does_not_persist(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    """unauthorized is in CONFIG_ERROR_CODES by default — no monkeypatch needed."""
    store = MemoryBookingStore()
    client = fake_client(
        [
            _business("unauthorized", http_status=401),
            _business("unauthorized", http_status=401),
        ]
    )
    clock = make_clock()
    cfg = attempt_config(prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(window_open)
    assert result.status == "error"
    assert await store.all_for_profile(cfg.profile_name) == []


@pytest.mark.asyncio
async def test_unknown_code_lost_does_not_persist(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    """Unknown business codes fall back to lost — must not persist."""
    store = MemoryBookingStore()
    client = fake_client(
        [
            _business("unknown_xyz"),
            _business("unknown_xyz"),
        ]
    )
    clock = make_clock()
    cfg = attempt_config(prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(window_open)
    assert result.status == "lost"
    assert await store.all_for_profile(cfg.profile_name) == []


@pytest.mark.asyncio
async def test_window_passed_does_not_persist(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
) -> None:
    store = MemoryBookingStore()
    client = fake_client([])
    clock = make_clock(initial_utc=datetime(2026, 4, 23, 2, 5, 0, tzinfo=UTC))
    past_window = datetime(2026, 4, 23, 2, 0, 0, tzinfo=UTC)
    cfg = attempt_config()
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    result = await attempt.run(past_window)
    assert result.status == "error"
    assert await store.all_for_profile(cfg.profile_name) == []


@pytest.mark.asyncio
async def test_attempt_persists_with_record_hash_and_phase(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    store = MemoryBookingStore()
    client = fake_client([_booking(record_id=7, record_hash="rh7")])
    clock = make_clock()
    cfg = attempt_config(parallel_shots=1, prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    await attempt.run(window_open)
    rec = (await store.all_for_profile(cfg.profile_name))[0]
    assert rec.record_hash == "rh7"
    assert rec.phase == "window"
    assert rec.booked_at_utc.tzinfo is not None


@pytest.mark.asyncio
async def test_legacy_single_court_uses_court_ids_zero(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    store = MemoryBookingStore()
    client = fake_client([_booking(record_id=1)])
    clock = make_clock()
    cfg = attempt_config(court_ids=(555,), parallel_shots=1, prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    await attempt.run(window_open)
    rec = (await store.all_for_profile(cfg.profile_name))[0]
    assert rec.court_id == 555


@pytest.mark.asyncio
async def test_persisted_record_round_trips(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
    window_open: datetime,
) -> None:
    """Sanity: persisted record can be round-tripped via to_dict / from_dict."""
    store = MemoryBookingStore()
    client = fake_client([_booking(record_id=1)])
    clock = make_clock()
    cfg = attempt_config(parallel_shots=1, prearm_lead_s=10.0)
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock), store=store)
    await attempt.run(window_open)
    rec = (await store.all_for_profile(cfg.profile_name))[0]
    d = rec.to_dict()
    rebuilt = BookedSlot.from_dict(d)
    assert rebuilt == rec


@pytest.mark.asyncio
async def test_default_store_is_none(
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    attempt_config: Callable[..., AttemptConfig],
) -> None:
    cfg = attempt_config()
    attempt = BookingAttempt(
        cfg, as_altegio_client(fake_client([])), as_clock(make_clock())
    )
    assert attempt._store is None  # type: ignore[attr-defined]


def test_attempt_config_profile_name_required() -> None:
    with pytest.raises(TypeError):
        AttemptConfig(  # type: ignore[call-arg]
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=1,
            fullname="R",
            phone="77000",
        )


def test_attempt_config_invalid_profile_name_rejected() -> None:
    with pytest.raises(ValueError, match="profile_name"):
        AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=1,
            fullname="R",
            phone="77000",
            profile_name="UPPER",
        )


def test_attempt_config_empty_profile_name_rejected() -> None:
    with pytest.raises(ValueError, match="profile_name"):
        AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=1,
            fullname="R",
            phone="77000",
            profile_name="",
        )


_ = Any
