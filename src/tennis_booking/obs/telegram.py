"""Telegram notifier for booking events (win / timeout / lost).

Best-effort outbound only — failures are logged at WARN level and never raised
into callers. The booking engine treats notification as a side observation; a
Telegram outage must not fail or delay a booking attempt.

Disabled mode: when bot_token is None, chat_ids is empty, or enabled=False,
`send()` is a silent no-op (no log, no HTTP). This is the default for tests
and dev runs.
"""
from __future__ import annotations

import asyncio
import html
import re
from datetime import datetime

import httpx
import structlog

from tennis_booking.altegio import BookingResponse
from tennis_booking.common.tz import ALMATY

__all__ = [
    "TelegramNotifier",
    "disabled_notifier",
    "format_lost_message",
    "format_slot_for_user",
    "format_timeout_message",
    "format_win_message",
]

_logger = structlog.get_logger(__name__)
_TELEGRAM_API_BASE = "https://api.telegram.org"
_DEFAULT_TIMEOUT_S = 5.0
_WEEKDAY_ABBREV = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
# Redacts the bot-token segment from any URL-shaped string. httpx exception
# messages embed the full request URL (incl. /bot<TOKEN>/...), so we MUST scrub
# it before logging or the token leaks into journalctl.
_TOKEN_URL_RE = re.compile(r"/bot[^/\s]+/")


def _redact_token(text: str) -> str:
    """Replace `/bot<TOKEN>/` with `/bot<REDACTED>/` in any string."""
    return _TOKEN_URL_RE.sub("/bot<REDACTED>/", text)


class TelegramNotifier:
    """Async Telegram notifier. Best-effort — never raises into caller.

    Construction:
      bot_token: Telegram bot token (None → disabled).
      chat_ids: tuple of destination chat_ids; sends to each in order.
      client: optional pre-built httpx.AsyncClient (tests inject mocks). When
        None, a new client is constructed lazily per call (cheaper than holding
        an open connection for an event that may fire once a week).
      timeout_s: per-request timeout. 5s is generous for Telegram API.
      enabled: explicit feature flag. When False, send() is a silent no-op
        regardless of token/chat_ids.
    """

    def __init__(
        self,
        bot_token: str | None,
        chat_ids: tuple[str, ...],
        *,
        client: httpx.AsyncClient | None = None,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        enabled: bool = True,
    ) -> None:
        self._bot_token = bot_token
        self._chat_ids = tuple(chat_ids)
        self._client = client
        self._timeout_s = timeout_s
        self._enabled = enabled
        self._log = _logger.bind(component="telegram_notifier")

    @property
    def is_active(self) -> bool:
        """True when send() will attempt HTTP. Used by callers / tests for
        wiring assertions."""
        return bool(self._enabled and self._bot_token and self._chat_ids)

    async def send(self, text: str) -> None:
        """Send `text` to every configured chat_id. Failures are swallowed.

        Disabled state (no token / no chats / enabled=False) → silent no-op
        without warn-level log noise.
        """
        if not self.is_active:
            return
        for chat_id in self._chat_ids:
            try:
                await self._post_send_message(chat_id, text)
                self._log.info("telegram_sent", chat_id=chat_id)
            except asyncio.CancelledError:
                raise
            except httpx.HTTPStatusError as exc:
                # 4xx/5xx response — log status + truncated body (≤200 chars).
                # Do NOT log str(exc) because it embeds the full request URL,
                # which contains the bot token in plaintext.
                body = exc.response.text[:200] if exc.response is not None else ""
                self._log.warning(
                    "telegram_send_failed",
                    chat_id=chat_id,
                    exc_type=type(exc).__name__,
                    status_code=exc.response.status_code if exc.response is not None else None,
                    body=_redact_token(body),
                )
            except httpx.RequestError as exc:
                # Connect/Timeout/etc. — log only the type. exc.args / str(exc)
                # may contain the request URL, so we deliberately drop them.
                self._log.warning(
                    "telegram_send_failed",
                    chat_id=chat_id,
                    exc_type=type(exc).__name__,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort by design
                # Last-resort catch-all: log only the exception type to avoid
                # any chance of a token-bearing message leaking through.
                self._log.warning(
                    "telegram_send_failed",
                    chat_id=chat_id,
                    exc_type=type(exc).__name__,
                )

    async def _post_send_message(self, chat_id: str, text: str) -> None:
        # Token is non-None when is_active=True; mypy needs the explicit assert.
        assert self._bot_token is not None
        url = f"{_TELEGRAM_API_BASE}/bot{self._bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if self._client is not None:
            response = await self._client.post(url, json=payload, timeout=self._timeout_s)
        else:
            async with httpx.AsyncClient(timeout=self._timeout_s) as client:
                response = await client.post(url, json=payload)
        # 4xx/5xx → raise so the caller-side except logs WARN. raise_for_status
        # returns httpx.HTTPStatusError which inherits from HTTPError → caught.
        response.raise_for_status()


def format_slot_for_user(slot_dt_local: datetime) -> str:
    """Human-readable slot label used in notification text bodies.

    Format: "Tue 05.05 20:00 (Almaty)". Always normalises to Asia/Almaty so
    the operator sees club-local time regardless of the input tzinfo.
    """
    if slot_dt_local.tzinfo is None:
        # Defensive — AttemptConfig validation rejects naive datetimes, but
        # the formatter is also used from the engine glue path which we don't
        # want to crash on a bad upstream caller.
        local = slot_dt_local.replace(tzinfo=ALMATY)
    else:
        local = slot_dt_local.astimezone(ALMATY)
    weekday = _WEEKDAY_ABBREV[local.weekday()]
    return f"{weekday} {local.strftime('%d.%m %H:%M')} (Almaty)"


def _common_lines(
    slot_dt_local: datetime,
    profile_name: str,
    pool_key: str | None,
    court_id: int | None,
) -> list[str]:
    # User-controlled string fields go through html.escape so that `<`, `&`, `>`
    # in YAML configs (e.g. pool_key="Evening <prime>") don't make Telegram
    # reject the message with 400 Bad Request under parse_mode=HTML. Template
    # tags like <b>/<code> below are NOT escaped — they're our own.
    lines = [
        f"slot: <code>{format_slot_for_user(slot_dt_local)}</code>",
        f"profile: {html.escape(profile_name)}",
    ]
    pool_part = html.escape(pool_key) if pool_key else "—"
    court_part = f"#{court_id}" if court_id is not None else ""
    pool_line = f"pool: {pool_part}".rstrip()
    if court_part:
        pool_line = f"{pool_line} {court_part}"
    lines.append(pool_line)
    return lines


def format_win_message(
    *,
    slot_dt_local: datetime,
    profile_name: str,
    pool_key: str | None,
    booking: BookingResponse,
    court_id: int,
    phase: str,
) -> str:
    lines = ["✅ <b>Бронь забронирована</b>"]
    lines.extend(_common_lines(slot_dt_local, profile_name, pool_key, court_id))
    lines.append(f"record: <code>{booking.record_id}</code>")
    lines.append(f"phase: {html.escape(phase)}")
    return "\n".join(lines)


def format_timeout_message(
    *,
    slot_dt_local: datetime,
    profile_name: str,
    pool_key: str | None,
    phase: str,
    duration_ms: float,
    shots_fired: int,
) -> str:
    lines = ["⏱️ <b>Не успели</b>"]
    lines.extend(_common_lines(slot_dt_local, profile_name, pool_key, court_id=None))
    lines.append(f"phase: {html.escape(phase)}")
    lines.append(f"duration: {int(duration_ms)}ms")
    lines.append(f"shots_fired: {shots_fired}")
    return "\n".join(lines)


def format_lost_message(
    *,
    slot_dt_local: datetime,
    profile_name: str,
    pool_key: str | None,
    business_code: str | None,
    phase: str,
) -> str:
    lines = ["❌ <b>Слот занят / отказан</b>"]
    lines.extend(_common_lines(slot_dt_local, profile_name, pool_key, court_id=None))
    lines.append(f"phase: {html.escape(phase)}")
    code_part = html.escape(business_code) if business_code else "—"
    lines.append(f"code: {code_part}")
    return "\n".join(lines)


def disabled_notifier() -> TelegramNotifier:
    """Construct a no-op notifier — used as the default for engine/scheduler
    constructors so tests and unconfigured dev runs work without ceremony.
    """
    return TelegramNotifier(bot_token=None, chat_ids=(), enabled=False)
