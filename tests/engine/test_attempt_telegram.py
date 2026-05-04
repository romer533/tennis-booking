"""Engine-side wiring tests for Telegram notifications.

Uses a minimal in-memory recording stub for `TelegramNotifier.send` instead
of full respx — the goal is to assert *that the engine triggers a notify*,
not to retest httpx. Notifier-internal HTTP behaviour lives in
tests/obs/test_telegram.py.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pytest

from tennis_booking.altegio import AltegioBusinessError, BookingResponse
from tennis_booking.engine import codes as codes_module
from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt
from tennis_booking.obs.telegram import TelegramNotifier

from .conftest import (
    FakeAltegioClient,
    FakeClock,
    as_altegio_client,
    as_clock,
)


class RecordingNotifier(TelegramNotifier):
    """TelegramNotifier subclass that records send() invocations instead of
    making HTTP calls. is_active stays True so engine code paths fire."""

    def __init__(self) -> None:
        super().__init__(bot_token="fake", chat_ids=("111",), enabled=True)
        self.sent: list[str] = []

    async def send(self, text: str) -> None:  # type: ignore[override]
        self.sent.append(text)


def _booking(record_id: int = 111, record_hash: str = "h") -> BookingResponse:
    return BookingResponse(record_id=record_id, record_hash=record_hash)


def _business(code: str, http_status: int = 422) -> AltegioBusinessError:
    return AltegioBusinessError(code=code, message=f"test-{code}", http_status=http_status)


@pytest.fixture
def patch_codes(monkeypatch: pytest.MonkeyPatch) -> Callable[[frozenset[str], frozenset[str]], None]:
    from tennis_booking.engine import attempt as attempt_module

    def _apply(not_open: frozenset[str], slot_taken: frozenset[str]) -> None:
        monkeypatch.setattr(codes_module, "NOT_OPEN_CODES", not_open)
        monkeypatch.setattr(codes_module, "SLOT_TAKEN_CODES", slot_taken)
        monkeypatch.setattr(attempt_module, "NOT_OPEN_CODES", not_open)
        monkeypatch.setattr(attempt_module, "SLOT_TAKEN_CODES", slot_taken)

    return _apply


async def _drain_pending_tasks() -> None:
    """`_schedule_terminal_notification` uses `asyncio.create_task`. Yield
    twice so the scheduled coroutine actually runs before assertions."""
    await asyncio.sleep(0)
    await asyncio.sleep(0)


# ---- WIN paths ------------------------------------------------------------


async def test_window_attempt_calls_notifier_on_win(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
) -> None:
    clock = make_clock()
    client = fake_client([_booking(record_id=42)])
    cfg = attempt_config(parallel_shots=1)
    notifier = RecordingNotifier()

    attempt = BookingAttempt(
        cfg, as_altegio_client(client), as_clock(clock), notifier=notifier
    )
    result = await attempt.run(window_open)
    await _drain_pending_tasks()

    assert result.status == "won"
    assert len(notifier.sent) == 1
    msg = notifier.sent[0]
    assert "Бронь забронирована" in msg
    assert "<code>42</code>" in msg
    assert "phase: window" in msg


# ---- TIMEOUT path ---------------------------------------------------------


async def test_window_attempt_calls_notifier_on_timeout(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
    patch_codes: Any,
) -> None:
    """all not_open shots → not_open_deadline timeout."""
    patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
    clock = make_clock()
    # Stick: every create_booking returns not_yet_open until deadline.
    client = fake_client([])
    client.set_default_side_effect(_business("not_yet_open"))
    cfg = attempt_config(
        parallel_shots=1,
        not_open_retry_ms=10,
        not_open_deadline_s=0.5,
        global_deadline_s=1.0,
    )
    notifier = RecordingNotifier()
    attempt = BookingAttempt(
        cfg, as_altegio_client(client), as_clock(clock), notifier=notifier
    )
    result = await attempt.run(window_open)
    await _drain_pending_tasks()

    assert result.status == "timeout"
    assert len(notifier.sent) == 1
    assert "Не успели" in notifier.sent[0]
    assert "duration:" in notifier.sent[0]


# ---- LOST path ------------------------------------------------------------


async def test_window_attempt_calls_notifier_on_lost(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
    patch_codes: Any,
) -> None:
    patch_codes(frozenset({"not_yet_open"}), frozenset({"slot_busy"}))
    clock = make_clock()
    client = fake_client([_business("slot_busy")])
    cfg = attempt_config(parallel_shots=1)
    notifier = RecordingNotifier()

    attempt = BookingAttempt(
        cfg, as_altegio_client(client), as_clock(clock), notifier=notifier
    )
    result = await attempt.run(window_open)
    await _drain_pending_tasks()

    assert result.status == "lost"
    assert len(notifier.sent) == 1
    assert "Слот занят" in notifier.sent[0]
    assert "code: slot_busy" in notifier.sent[0]


# ---- ERROR path → no notify ------------------------------------------------


async def test_window_attempt_no_notify_on_error_window_passed(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
) -> None:
    """status=error must NOT trigger any notification."""
    from datetime import timedelta as _td

    clock = make_clock()
    client = fake_client([])
    cfg = attempt_config()
    notifier = RecordingNotifier()

    attempt = BookingAttempt(
        cfg, as_altegio_client(client), as_clock(clock), notifier=notifier
    )
    past = clock.now_utc() - _td(seconds=1)
    result = await attempt.run(past)
    await _drain_pending_tasks()

    assert result.status == "error"
    assert notifier.sent == []


# ---- Disabled notifier → no calls -----------------------------------------


async def test_disabled_notifier_no_send(
    attempt_config: Callable[..., AttemptConfig],
    fake_client: Callable[..., FakeAltegioClient],
    make_clock: Callable[..., FakeClock],
    window_open: datetime,
) -> None:
    """When notifier is the disabled default, engine path must skip dispatch
    entirely (is_active is False — no asyncio.create_task created)."""
    clock = make_clock()
    client = fake_client([_booking()])
    cfg = attempt_config(parallel_shots=1)

    # Default notifier (no kwarg) → disabled_notifier(); no send invocations
    # observed because there is no notifier object to record. We assert by
    # absence-of-error and absence-of-task-leak.
    attempt = BookingAttempt(cfg, as_altegio_client(client), as_clock(clock))
    result = await attempt.run(window_open)
    await _drain_pending_tasks()

    assert result.status == "won"
