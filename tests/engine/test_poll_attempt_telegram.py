"""Engine-side wiring tests for Telegram notifications in poll mode."""
from __future__ import annotations

import asyncio
from datetime import UTC, timedelta
from typing import Any

from tennis_booking.altegio import BookingResponse, TimeSlot
from tennis_booking.engine.attempt import AttemptConfig
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.obs.telegram import TelegramNotifier

from .conftest import SERVICE_ID, SLOT, STAFF_ID, FakeClock
from .test_poll_attempt import FakePollClient, _slot, as_client


class RecordingNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(bot_token="fake", chat_ids=("111",), enabled=True)
        self.sent: list[str] = []

    async def send(self, text: str) -> None:  # type: ignore[override]
        self.sent.append(text)


def _attempt_config(**overrides: Any) -> AttemptConfig:
    defaults: dict[str, Any] = {
        "slot_dt_local": SLOT,
        "court_ids": (STAFF_ID,),
        "service_id": SERVICE_ID,
        "fullname": "Roman",
        "phone": "77026473809",
        "profile_name": "roman",
        "email": None,
    }
    defaults.update(overrides)
    return AttemptConfig(**defaults)


def _start_clock() -> FakeClock:
    initial = (SLOT - timedelta(days=2)).astimezone(UTC)
    return FakeClock(initial_utc=initial, initial_mono=1000.0)


async def _drain_pending_tasks() -> None:
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def test_poll_attempt_calls_notifier_on_win() -> None:
    clock = _start_clock()
    bookable: list[TimeSlot] = [_slot(SLOT, is_bookable=True, staff_id=STAFF_ID)]
    client = FakePollClient(
        search_effects=[bookable],
        booking_effects=[BookingResponse(record_id=999, record_hash="h")],
    )
    cfg = _attempt_config()
    poll = PollConfigData(interval_s=60, start_offset_days=2)
    notifier = RecordingNotifier()

    attempt = PollAttempt(cfg, poll, as_client(client), clock, notifier=notifier)
    result = await attempt.run()
    await _drain_pending_tasks()

    assert result.status == "won"
    assert len(notifier.sent) == 1
    assert "Бронь забронирована" in notifier.sent[0]
    assert "<code>999</code>" in notifier.sent[0]
    assert "phase: poll" in notifier.sent[0]


async def test_poll_attempt_does_not_notify_won_by_sibling() -> None:
    """A poll losing to a window-sibling that already won → no notification.
    Cross-task dedup is a normal interaction, not user-actionable."""
    clock = _start_clock()
    client = FakePollClient(search_effects=[], booking_effects=[])
    cfg = _attempt_config()
    poll = PollConfigData(interval_s=60, start_offset_days=2)
    notifier = RecordingNotifier()

    won_event = asyncio.Event()
    won_event.set()  # sibling already won before poll's first tick

    attempt = PollAttempt(
        cfg, poll, as_client(client), clock, won_event=won_event, notifier=notifier
    )
    result = await attempt.run()
    await _drain_pending_tasks()

    assert result.status == "lost"
    assert result.business_code == "won_by_sibling"
    assert notifier.sent == []
