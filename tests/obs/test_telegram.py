from __future__ import annotations

from datetime import datetime

import httpx
import pytest
import respx
import structlog

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


# ---- Security regressions -------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_log_does_not_contain_bot_token_on_4xx() -> None:
    """A 4xx response from Telegram must not leak the bot token in WARN logs.

    httpx.HTTPStatusError carries the request URL inside its message, which
    contains `bot<TOKEN>` in plaintext. Logging `str(exc)` would dump it to
    journalctl. We must log only sanitized fields.

    The body is also passed through `_redact_token` defensively in case Telegram
    or an intermediate proxy echoes the URL in the response body.
    """
    # Body intentionally echoes the bot URL to exercise the redactor path.
    body_with_token = (
        f"401 Unauthorized for {_TG_API}/bot{_TOKEN}/sendMessage"
    )
    respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        return_value=httpx.Response(401, text=body_with_token),
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    with structlog.testing.capture_logs() as captured:
        await n.send("hello")
    # We expect at least one WARN entry for the failed send.
    warns = [e for e in captured if e.get("log_level") == "warning"]
    assert warns, "expected at least one WARN log for 4xx response"
    for entry in warns:
        for key, value in entry.items():
            text = str(value)
            # The `bot<TOKEN>/` URL segment is the leak we MUST prevent.
            assert f"bot{_TOKEN}/" not in text, (
                f"`bot<token>/` segment leaked in log field {key!r}: {text!r}"
            )
            # And the redacted marker should be present in the body field.
        if "body" in entry:
            assert "<REDACTED>" in str(entry["body"]) or _TOKEN not in str(entry["body"])


@pytest.mark.asyncio
@respx.mock
async def test_log_does_not_contain_bot_token_on_connect_error() -> None:
    """ConnectError str() also embeds the request URL — must NOT be logged."""
    respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        side_effect=httpx.ConnectError(
            f"failed connecting to {_TG_API}/bot{_TOKEN}/sendMessage",
        )
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    with structlog.testing.capture_logs() as captured:
        await n.send("hello")
    warns = [e for e in captured if e.get("log_level") == "warning"]
    assert warns
    for entry in warns:
        for key, value in entry.items():
            assert _TOKEN not in str(value), (
                f"bot token leaked in log field {key!r}"
            )


@pytest.mark.asyncio
@respx.mock
async def test_message_html_escapes_user_fields() -> None:
    """User-controlled fields (pool_key, profile_name, business_code, phase)
    with `<`, `&`, `>` MUST be HTML-escaped — otherwise Telegram rejects the
    payload with 400 under parse_mode=HTML.

    Template tags like <b> and <code> stay raw because they're our own.
    """
    route = respx.post(f"{_TG_API}/bot{_TOKEN}/sendMessage").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    booking = BookingResponse(record_id=99, record_hash="h")
    text = format_win_message(
        slot_dt_local=_SLOT,
        profile_name="roman & co",
        pool_key="Evening <prime>",
        booking=booking,
        court_id=42,
        phase="window <test>",
    )
    n = TelegramNotifier(bot_token=_TOKEN, chat_ids=(_CHAT_A,), enabled=True)
    await n.send(text)
    body = route.calls[0].request.read().decode("utf-8")
    # User input is escaped:
    assert "Evening &lt;prime&gt;" in body
    assert "roman &amp; co" in body
    assert "window &lt;test&gt;" in body
    # And raw `<prime>` / `<test>` from user input MUST NOT appear:
    assert "<prime>" not in body
    assert "window <test>" not in body
    # Our own template tags are still present (un-escaped):
    assert "<b>" in body
    assert "<code>" in body


def test_format_lost_message_html_escapes_business_code() -> None:
    text = format_lost_message(
        slot_dt_local=_SLOT,
        profile_name="r",
        pool_key="p",
        business_code="bad <html>",
        phase="window",
    )
    assert "bad &lt;html&gt;" in text
    assert "bad <html>" not in text


def test_format_message_html_escapes_pool_key_with_ampersand() -> None:
    text = format_timeout_message(
        slot_dt_local=_SLOT,
        profile_name="r",
        pool_key="A & B",
        phase="poll",
        duration_ms=100,
        shots_fired=1,
    )
    assert "A &amp; B" in text
    assert "pool: A & B\n" not in text  # raw ampersand line not present
