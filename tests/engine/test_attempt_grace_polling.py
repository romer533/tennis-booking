"""Тесты engine grace polling — позднее открытие слота админом.

Сценарий: T−0..T+10s исчерпан, ВСЕ shots вернули service_not_available
(NOT_OPEN_CODES). При наличии grace_polling — engine продолжает каждые
interval_s опрашивать search/timeslots; на первый bookable — fan-out
create_booking. До истечения period_s или config_err.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime

import pytest

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioTransportError,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.config.schema import GracePollingConfig
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt

from .conftest import (
    SLOT,
    STAFF_ID,
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)

# ---- helpers ---------------------------------------------------------------


def _booking(record_id: int = 111, record_hash: str = "h") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


def _snv(http_status: int = 422) -> AltegioBusinessError:
    """service_not_available bizness error."""
    return AltegioBusinessError(
        code="service_not_available",
        message="The service is not available at the selected time",
        http_status=http_status,
    )


def _bookable_slot(staff_id: int | None = None, dt: datetime | None = None) -> TimeSlot:
    return TimeSlot(dt=dt or SLOT, is_bookable=True, staff_id=staff_id)


def _not_bookable_slot(staff_id: int | None = None, dt: datetime | None = None) -> TimeSlot:
    return TimeSlot(dt=dt or SLOT, is_bookable=False, staff_id=staff_id)


def _grace_polling(period_s: int = 120, interval_s: int = 10) -> GracePollingConfig:
    return GracePollingConfig(period_s=period_s, interval_s=interval_s)


# ---- G7: no grace if win ---------------------------------------------------


class TestG7NoGraceIfWin:
    async def test_initial_win_skips_grace_no_search(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([_booking(42)])
        cfg = attempt_config(parallel_shots=1, grace_polling=_grace_polling())

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 42
        assert len(client.search_timeslots_calls) == 0


# ---- G13: backward compat ---------------------------------------------------


class TestG13BackwardCompat:
    async def test_grace_polling_none_existing_behavior(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """grace_polling=None → existing timeout behavior."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())
        cfg = attempt_config(
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=None,
        )
        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"
        assert result.business_code == "service_not_available"
        assert len(client.search_timeslots_calls) == 0


# ---- G1: happy path -- snv → grace → search bookable → fire → won ----------


class TestG1GraceHappyPath:
    async def test_all_snv_then_grace_first_iter_wins(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        # Initial fire phase: all 4 shots return snv (sticky default). After
        # not_open_deadline, grace mode entered. First search → bookable. Once
        # grace fire is detected, switch sticky default to booking responses
        # so that the 4 fan-out shots return successes.
        court_ids = (STAFF_ID, STAFF_ID + 1, STAFF_ID + 2, STAFF_ID + 3)
        client = fake_client([])
        client.set_default_side_effect(_snv())

        # Search returns bookable on first iter; we flip create_booking default
        # to booking response right at search time.
        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(101))
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=court_ids,
            parallel_shots=4,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won", f"expected won, got {result.status} (code={result.business_code})"
        assert result.phase == "window"  # grace persists with phase=window per spec
        assert result.booking is not None
        assert result.booking.record_id == 101
        assert len(client.search_timeslots_calls) >= 1


# ---- G3: grace timeout — period_s exhausted --------------------------------


class TestG3GraceTimeout:
    async def test_no_match_until_period_exhausted(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())
        # Search effects: many "no match" responses; loop continues until period_s exhausted.
        client.add_search(*[[_not_bookable_slot()] for _ in range(20)])

        cfg = attempt_config(
            court_ids=(STAFF_ID, STAFF_ID + 1, STAFF_ID + 2, STAFF_ID + 3),
            parallel_shots=4,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=60, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"
        assert result.business_code == "grace_period_exhausted"
        assert len(client.search_timeslots_calls) >= 1


# ---- G16: grace triggers on ANY service_not_available (was: requires ALL) --
# Изменено в incident 26.04: parser fall-through + relaxed grace trigger.
# Mix snv + unknown_code больше не блокирует grace — unknown reclassified
# как часть not_open потока.


class TestG16GraceTriggersOnAnySnv:
    async def test_mix_snv_and_unknown_triggers_grace(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """3 snv + 1 unknown → grace triggered (incident 26.04 N3 regression).

        Раньше unknown_code блокировал grace fallback. Теперь — silent reclassify
        unknown → not_open retry/grace путь.
        """
        clock = make_clock()
        client = fake_client(
            [
                _snv(),
                _snv(),
                _snv(),
                AltegioBusinessError(code="weird_code", message="x", http_status=422),
            ]
        )
        # После исчерпания скрипта — sticky default snv (для retries в not_open loop).
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(2001))
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=(STAFF_ID, STAFF_ID + 1, STAFF_ID + 2, STAFF_ID + 3),
            parallel_shots=4,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 2001
        assert len(client.search_timeslots_calls) >= 1

    async def test_one_snv_six_unknown_triggers_grace(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """1 snv + 6 unknown → grace triggered (any single snv is enough)."""
        clock = make_clock()
        client = fake_client(
            [
                _snv(),
                AltegioBusinessError(code="weird1", message="x", http_status=422),
                AltegioBusinessError(code="weird2", message="x", http_status=422),
                AltegioBusinessError(code="weird3", message="x", http_status=422),
                AltegioBusinessError(code="weird4", message="x", http_status=422),
                AltegioBusinessError(code="weird5", message="x", http_status=422),
                AltegioBusinessError(code="weird6", message="x", http_status=422),
            ]
        )
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(2002))
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=tuple(STAFF_ID + i for i in range(7)),
            parallel_shots=7,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 2002

    async def test_all_unknown_no_snv_falls_back_to_lost(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """0 snv + 7 unknown → fallback "lost" (existing behavior preserved).

        Без хотя бы одного snv нет основания думать, что слот ещё откроется.
        """
        clock = make_clock()
        client = fake_client(
            [
                AltegioBusinessError(code="weird_code", message="x", http_status=422)
                for _ in range(7)
            ]
        )
        cfg = attempt_config(
            court_ids=tuple(STAFF_ID + i for i in range(7)),
            parallel_shots=7,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "lost"
        assert result.business_code == "weird_code"
        assert len(client.search_timeslots_calls) == 0

    async def test_slot_taken_overrides_mix_snv_unknown(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """1 snv + 1 slot_taken + 5 unknown → status=lost, code=slot_taken (existing priority)."""
        clock = make_clock()
        # Inject one synthetic slot_taken via a fake code that is in SLOT_TAKEN_CODES.
        # Currently SLOT_TAKEN_CODES is empty, so we can't realistically test
        # a slot_taken priority short of monkeypatching. Use monkeypatch.
        import tennis_booking.engine.attempt as attempt_mod

        original = attempt_mod.SLOT_TAKEN_CODES
        attempt_mod.SLOT_TAKEN_CODES = frozenset({"slot_taken"})  # type: ignore[misc]
        try:
            client = fake_client(
                [
                    _snv(),
                    AltegioBusinessError(code="slot_taken", message="x", http_status=422),
                    AltegioBusinessError(code="weird1", message="x", http_status=422),
                    AltegioBusinessError(code="weird2", message="x", http_status=422),
                    AltegioBusinessError(code="weird3", message="x", http_status=422),
                    AltegioBusinessError(code="weird4", message="x", http_status=422),
                    AltegioBusinessError(code="weird5", message="x", http_status=422),
                ]
            )
            cfg = attempt_config(
                court_ids=tuple(STAFF_ID + i for i in range(7)),
                parallel_shots=7,
                not_open_retry_ms=100,
                not_open_deadline_s=1.0,
                global_deadline_s=2.0,
                grace_polling=_grace_polling(period_s=120, interval_s=10),
            )

            attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
            result = await attempt.run(window_open)

            assert result.status == "lost"
            assert result.business_code == "slot_taken"
            assert len(client.search_timeslots_calls) == 0
        finally:
            attempt_mod.SLOT_TAKEN_CODES = original  # type: ignore[misc]

    async def test_config_err_overrides_mix_snv_unknown(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """1 snv + 1 unauthorized + 5 unknown → status=error, code=unauthorized (existing)."""
        clock = make_clock()
        client = fake_client(
            [
                _snv(),
                AltegioBusinessError(code="unauthorized", message="x", http_status=401),
                AltegioBusinessError(code="weird1", message="x", http_status=422),
                AltegioBusinessError(code="weird2", message="x", http_status=422),
                AltegioBusinessError(code="weird3", message="x", http_status=422),
                AltegioBusinessError(code="weird4", message="x", http_status=422),
                AltegioBusinessError(code="weird5", message="x", http_status=422),
            ]
        )
        cfg = attempt_config(
            court_ids=tuple(STAFF_ID + i for i in range(7)),
            parallel_shots=7,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "error"
        assert result.business_code == "unauthorized"
        assert len(client.search_timeslots_calls) == 0

    async def test_mix_snv_unknown_grace_none_timeouts(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Mix snv + unknown с grace_polling=None → старое поведение для snv:
        not_open retries → timeout="not_open_deadline" (без grace)."""
        clock = make_clock()
        client = fake_client(
            [
                _snv(),
                _snv(),
                AltegioBusinessError(code="weird", message="x", http_status=422),
                AltegioBusinessError(code="weird", message="x", http_status=422),
            ]
        )
        client.set_default_side_effect(_snv())

        cfg = attempt_config(
            court_ids=(STAFF_ID, STAFF_ID + 1, STAFF_ID + 2, STAFF_ID + 3),
            parallel_shots=4,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=None,
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"
        assert result.business_code == "service_not_available"


# ---- Additional grace state-machine tests ----------------------------------


class TestGraceStateMachine:
    async def test_grace_search_no_match_then_match_wins(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(200))
            return [_bookable_slot()]

        client.add_search(
            [_not_bookable_slot()],
            [_not_bookable_slot()],
            bookable_then_flip,
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID, STAFF_ID + 1),
            parallel_shots=2,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 200
        assert len(client.search_timeslots_calls) == 3

    async def test_grace_search_returns_empty_list_no_match(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())
        client.add_search(*[[] for _ in range(10)])

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=60, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "timeout"
        assert result.business_code == "grace_period_exhausted"

    async def test_grace_search_transport_err_retried(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Transport err during search → log warn, continue polling, не break grace."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(300))
            return [_bookable_slot()]

        client.add_search(
            AltegioTransportError("ReadTimeout"),
            AltegioTransportError("ConnectError"),
            bookable_then_flip,
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 300
        assert len(client.search_timeslots_calls) == 3

    async def test_grace_search_business_err_non_config_retried(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Non-config business err during search → continue polling."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(400))
            return [_bookable_slot()]

        client.add_search(
            AltegioBusinessError(code="weird", message="m", http_status=400),
            bookable_then_flip,
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None

    async def test_grace_search_config_err_exits_immediately(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Config err (unauthorized) during search → exit error."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())
        client.add_search(
            AltegioBusinessError(code="unauthorized", message="m", http_status=401),
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=60, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "error"
        assert result.business_code == "unauthorized"

    async def test_grace_search_unknown_exception_retried(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(500))
            return [_bookable_slot()]

        client.add_search(
            ValueError("unexpected"),
            bookable_then_flip,
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"

    async def test_grace_fire_all_snv_continues_polling(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """В grace fire всё ещё snv (race: search видел bookable, но другой клиент успел).
        Должны продолжать polling."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        # 2 search returns bookable, but stays snv until 2nd search; then flip to win.
        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(600))
            return [_bookable_slot()]

        client.add_search(
            [_bookable_slot()],  # 1st: bookable but fire returns snv (race)
            bookable_then_flip,  # 2nd: bookable, switch default to win
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID, STAFF_ID + 1),
            parallel_shots=2,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 600
        assert len(client.search_timeslots_calls) == 2

    async def test_grace_fire_transport_only_continues_polling(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def first_search() -> list[TimeSlot]:
            client.set_default_side_effect(AltegioTransportError("ReadTimeout"))
            return [_bookable_slot()]

        async def second_search() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(700))
            return [_bookable_slot()]

        client.add_search(first_search, second_search)

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 700

    async def test_grace_fire_config_err_exits_error(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(
                AltegioBusinessError(code="unauthorized", message="m", http_status=401)
            )
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=60.0,
            grace_polling=_grace_polling(period_s=120, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "error"
        assert result.business_code == "unauthorized"

    async def test_grace_fire_unknown_continues_polling(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Unknown business code in grace fire → status=lost, but caller continues
        polling (since we're in grace loop). Eventually gets a win."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def first_search() -> list[TimeSlot]:
            client.set_default_side_effect(
                AltegioBusinessError(code="weird", message="x", http_status=422)
            )
            return [_bookable_slot()]

        async def second_search() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(800))
            return [_bookable_slot()]

        client.add_search(first_search, second_search)

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.booking is not None
        assert result.booking.record_id == 800

    async def test_grace_fire_fans_out_all_court_ids(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Grace fire должен отправить shots на ВСЕ court_ids (4 courts → 4 shots)."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(900))
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=(10, 11, 12, 13),
            parallel_shots=4,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        # Last 4 calls — grace fire phase (4 distinct courts).
        last4 = client.create_booking_calls[-4:]
        staff_ids_in_grace = {c["staff_id"] for c in last4}
        assert staff_ids_in_grace == {10, 11, 12, 13}

    async def test_grace_search_uses_correct_date_and_staff_ids(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(1000))
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=(50, 51, 52),
            parallel_shots=3,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        await attempt.run(window_open)

        first_search = client.search_timeslots_calls[0]
        assert first_search["date_local"] == SLOT.date()
        assert set(first_search["staff_ids"]) == {50, 51, 52}
        assert first_search["timeout_s"] == 5.0

    async def test_grace_search_match_filters_by_staff_id(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Slot с staff_id=999 (не в наших court_ids) — НЕ матч."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(1100))
            return [_bookable_slot(staff_id=10)]

        client.add_search(
            [_bookable_slot(staff_id=999)],  # staff_id not in court_ids — skipped
            bookable_then_flip,
        )

        cfg = attempt_config(
            court_ids=(10,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert len(client.search_timeslots_calls) == 2

    async def test_grace_search_match_filters_by_dt(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Slot с другим временем — не матч."""
        from tennis_booking.common.tz import ALMATY

        wrong_dt = datetime(2026, 4, 26, 22, 0, 0, tzinfo=ALMATY)  # 22:00 vs SLOT 23:00
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(1200))
            return [_bookable_slot()]

        client.add_search(
            [_bookable_slot(dt=wrong_dt)],
            bookable_then_flip,
        )

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert len(client.search_timeslots_calls) == 2

    async def test_grace_search_match_with_null_staff_id(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Slot с staff_id=None (server-side filter applied) — считается match."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(1300))
            return [_bookable_slot(staff_id=None)]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"

    async def test_grace_phase_persists_with_window_phase(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """Win in grace → result.phase == 'window' (per spec; persistence preserves enum)."""
        clock = make_clock()
        client = fake_client([])
        client.set_default_side_effect(_snv())

        async def bookable_then_flip() -> list[TimeSlot]:
            client.set_default_side_effect(_booking(1400))
            return [_bookable_slot()]

        client.add_search(bookable_then_flip)

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=180, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        result = await attempt.run(window_open)

        assert result.status == "won"
        assert result.phase == "window"

    async def test_grace_cancellation_propagates(
        self,
        attempt_config: Callable[..., AttemptConfig],
        fake_client: Callable[..., FakeAltegioClient],
        make_clock: Callable[..., FakeClock],
        window_open: datetime,
    ) -> None:
        """External cancel during grace must propagate."""
        clock = make_clock()

        async def hang_search() -> list[TimeSlot]:
            await asyncio.sleep(3600)
            return [_bookable_slot()]

        client = fake_client([])
        client.set_default_side_effect(_snv())
        client.add_search(hang_search)

        cfg = attempt_config(
            court_ids=(STAFF_ID,),
            parallel_shots=1,
            not_open_retry_ms=100,
            not_open_deadline_s=1.0,
            global_deadline_s=2.0,
            grace_polling=_grace_polling(period_s=600, interval_s=10),
        )

        attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(attempt.run(window_open), timeout=0.5)


# ---- AttemptConfig grace_polling field validation --------------------------


class TestAttemptConfigGracePolling:
    def test_default_none(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=1,
            fullname="Roman",
            phone="77000",
            profile_name="roman",
        )
        assert cfg.grace_polling is None

    def test_with_grace_polling(self) -> None:
        cfg = AttemptConfig(
            slot_dt_local=SLOT,
            court_ids=(1,),
            service_id=1,
            fullname="Roman",
            phone="77000",
            profile_name="roman",
            grace_polling=GracePollingConfig(period_s=120, interval_s=10),
        )
        assert cfg.grace_polling is not None
        assert cfg.grace_polling.period_s == 120
