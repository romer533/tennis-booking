from __future__ import annotations

from datetime import datetime

import httpx
import pytest
import respx

from tennis_booking.altegio import BookingResponse
from tennis_booking.common.tz import ALMATY
from tennis_booking.obs.telegram import (
    TelegramNotifier,
    disabled_notifier,
    format_lost_message,
    format_slot_for_user,
    format_timeout_message,
    format_win_message,
)

_TG_API = "https://api.telegram.org"
_TOKEN = "123456:fake-token"
_CHAT_A = "111"
_CHAT_B = "-222333"
_SLOT = datetime(2026, 5, 5, 20, 0, tzinfo=ALMATY)


# ---- formatting helpers ---------------------------------------------------


def test_format_slot_for_user_almaty() -> None:
    text = format_slot_for_user(_SLOT)
    # 2026-05-05 is a Tuesday in any TZ
    assert text == "Tue 05.05 20:00 (Almaty)"


def test_format_slot_handles_naive_datetime() -> None:
    naive = datetime(2026, 5, 5, 20, 0)
    text = format_slot_for_user(naive)
    assert text == "Tue 05.05 20:00 (Almaty)"


def test_format_win_message_includes_all_fields() -> None:
    booking = BookingResponse(record_id=12345, record_hash="abc")
    text = format_win_message(
        slot_dt_local=_SLOT,
        profile_name="roman",
        pool_key="evening_pool",
        booking=booking,
        court_id=42,
        phase="window",
    )
    assert "Бронь забронирована" in text
    assert "<code>Tue 05.05 20:00 (Almaty)</code>" in text
    assert "profile: roman" in text
    assert "pool: evening_pool #42" in text
    assert "<code>12345</code>" in text
    assert "phase: window" in text


def test_format_timeout_message_fields() -> None:
    text = format_timeout_message(
        slot_dt_local=_SLOT,
        profile_name="roman",
        pool_key="pool_x",
        phase="poll",
        duration_ms=8421.5,
        shots_fired=4,
    )
    assert "Не успели" in text
    assert "phase: poll" in text
    assert "duration: 8421ms" in text
    assert "shots_fired: 4" in text


def test_format_lost_message_with_business_code() -> None:
    text = format_lost_message(
        slot_dt_local=_SLOT,
        profile_name="roman",
        pool_key=None,
        business_code="record_busy",
        phase="window",
    )
    assert "Слот занят" in text
    assert "code: record_busy" in text
    assert "pool: —" in text


# ---- TelegramNotifier — disabled state ------------------------------------


def test_disabled_notifier_factory_is_inactive() -> None:
    n = disabled_notifier()
    assert n.is_active is False


@pytest.mark.asyncio
async def test_notifier_disabled_no_http_call() -> None:
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=False)
    # No respx mounting at all → if send() touches httpx, the call would
    # error out (real network attempt). is_active=False short-circuits.
    assert n.is_active is False
    await n.send("hello")  # no exception


@pytest.mark.asyncio
async def test_notifier_no_token_no_call() -> None:
    n = TelegramNotifier(bot_token=None, chat_ids=(_CHAT_A,), enabled=True)
    assert n.is_active is False
    await n.send("hello")


@pytest.mark.asyncio
async def test_notifier_no_chat_ids_no_call() -> None:
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(), enabled=True)
    assert n.is_active is False
    await n.send("hello")


# ---- TelegramNotifier — happy path / errors -------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_notifier_send_to_single_chat() -> None:
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    await n.send("hello")
    assert route.call_count == 1
    sent = route.calls[0].request
    body = sent.read().decode("utf-8")
    assert _CHAT_A in body
    assert '"parse_mode":"HTML"' in body
    assert '"disable_web_page_preview":true' in body


@pytest.mark.asyncio
@respx.mock
async def test_notifier_send_to_multiple_chats() -> None:
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A, _CHAT_B), enabled=True)
    await n.send("hello")
    assert route.call_count == 2
    bodies = [call.request.read().decode("utf-8") for call in route.calls]
    assert any(_CHAT_A in b for b in bodies)
    assert any(_CHAT_B in b for b in bodies)


@pytest.mark.asyncio
@respx.mock
async def test_notifier_failure_swallowed() -> None:
    """httpx-level error must NOT bubble out of send().

    The contract is "best-effort, never raises into caller". Internal WARN
    logging is verified by inspection of source / engine integration tests
    (where structlog → stdlib bridge is configured). Relying on caplog here
    is fragile because structlog only routes to stdlib once `setup_logging`
    has been called in the test session.
    """
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        side_effect=httpx.ConnectError("boom")
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    await n.send("hello")  # must not raise
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_notifier_5xx_response_swallowed() -> None:
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        return_value=httpx.Response(500, text="server error")
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    await n.send("hello")  # must not raise
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_notifier_timeout_swallowed() -> None:
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        side_effect=httpx.TimeoutException("timeout")
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    await n.send("hello")  # must not raise
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_notifier_message_format_html() -> None:
    """Body MUST include parse_mode=HTML so <b>, <code> renders."""
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    await n.send("<b>x</b>")
    body = route.calls[0].request.read().decode("utf-8")
    assert '"parse_mode":"HTML"' in body


@pytest.mark.asyncio
@respx.mock
async def test_notifier_continues_to_second_chat_after_first_fails() -> None:
    """One chat failing must not stop delivery to the other (best-effort fan-out)."""
    # respx side_effects evaluated in order; first call fails, second succeeds.
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        side_effect=[
            httpx.ConnectError("boom"),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A, _CHAT_B), enabled=True)
    await n.send("hello")
    assert route.call_count == 2
