"""Cloudflare challenge detection in client response parsing.

Production observation 28.04 02:00 UTC fire: 6% of book_record responses came
back as 403 + text/html with the standard Cloudflare interstitial. Old code
classified them as `AltegioBusinessError(code="unknown")` → engine fallback
"lost", no retry. Fix: detect challenge body, raise
`AltegioTransportError(cause="cloudflare_challenge")` so engine retries on
the existing transport path.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx
import pytest
import respx
from pydantic import SecretStr

from tennis_booking.altegio import (
    ALMATY,
    AltegioBusinessError,
    AltegioClient,
    AltegioConfig,
    AltegioTransportError,
)
from tennis_booking.altegio.client import BOOK_RECORD_PATH

BEARER = "test-bearer-secret"
COMPANY_ID = 521176
BOOKFORM_ID = 551098
BASE_URL = "https://b551098.alteg.io"
SERVICE_ID = 7849893
STAFF_ID = 1521566
SLOT = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
BOOK_PATH = BOOK_RECORD_PATH.format(company_id=COMPANY_ID)


# Truncated production-incident-style body. Real one was ~5866 bytes; this
# preserves the markers that matter (`Just a moment...` title + the
# `challenges.cloudflare.com` script src) without bloating the test file.
PROD_CLOUDFLARE_BODY = (
    '<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title>'
    '<meta http-equiv="content-security-policy" content="default-src \'none\'; '
    "script-src 'nonce-XXX' 'unsafe-eval' https://challenges.cloudflare.com; "
    'style-src \'unsafe-inline\'; img-src https:; connect-src https://challenges.cloudflare.com">'
    "</head><body>Verifying you are human...</body></html>"
)


def _make_config(**overrides: Any) -> AltegioConfig:
    kwargs: dict[str, Any] = {
        "bearer_token": SecretStr(BEARER),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "bookform_id": BOOKFORM_ID,
    }
    kwargs.update(overrides)
    return AltegioConfig(**kwargs)


async def _do_book() -> None:
    async with AltegioClient(_make_config()) as client:
        await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="Roman",
            phone="77026473809",
        )


# ---- Detection -------------------------------------------------------------


@respx.mock
async def test_cloudflare_challenge_classified_as_transport_error() -> None:
    """Точное body из production-инцидента 28.04 → AltegioTransportError(cause)."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403,
            content=PROD_CLOUDFLARE_BODY.encode("utf-8"),
            headers={"content-type": "text/html; charset=UTF-8"},
        )
    )
    with pytest.raises(AltegioTransportError) as ei:
        await _do_book()
    assert ei.value.cause == "cloudflare_challenge"


@respx.mock
async def test_cloudflare_just_a_moment_marker() -> None:
    """Body содержит только 'Just a moment...' (без 'challenges.cloudflare.com')."""
    body = (
        '<!DOCTYPE html><html><head><title>Just a moment...</title></head>'
        "<body>Checking your browser...</body></html>"
    )
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403, content=body.encode(), headers={"content-type": "text/html"}
        )
    )
    with pytest.raises(AltegioTransportError) as ei:
        await _do_book()
    assert ei.value.cause == "cloudflare_challenge"


@respx.mock
async def test_cloudflare_challenges_url_marker() -> None:
    """Body содержит только 'challenges.cloudflare.com' (без 'Just a moment...')."""
    body = (
        "<html><head><title>Access denied</title></head>"
        '<body><script src="https://challenges.cloudflare.com/turnstile/v0/api.js">'
        "</script></body></html>"
    )
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403, content=body.encode(), headers={"content-type": "text/html"}
        )
    )
    with pytest.raises(AltegioTransportError) as ei:
        await _do_book()
    assert ei.value.cause == "cloudflare_challenge"


@respx.mock
async def test_cloudflare_case_insensitive_markers() -> None:
    """Маркеры — case-insensitive."""
    variants = (
        "<title>JUST A MOMENT...</title>",
        "<title>just a moment...</title>",
        "<title>Just A Moment...</title>",
        '<script src="https://CHALLENGES.CLOUDFLARE.COM/x.js"></script>',
        '<script src="https://challenges.cloudflare.com/x.js"></script>',
    )
    for body in variants:
        with respx.mock(assert_all_called=False) as router:
            router.post(f"{BASE_URL}{BOOK_PATH}").mock(
                return_value=httpx.Response(
                    403,
                    content=f"<html><body>{body}</body></html>".encode(),
                    headers={"content-type": "text/html"},
                )
            )
            with pytest.raises(AltegioTransportError) as ei:
                await _do_book()
            assert ei.value.cause == "cloudflare_challenge", (
                f"failed for variant: {body!r}"
            )


# ---- Negative — preserve existing behaviour --------------------------------


@respx.mock
async def test_403_html_without_cloudflare_markers_remains_unknown() -> None:
    """403 + text/html но без Cloudflare маркеров → existing path (unknown business)."""
    body = "<html><body>Forbidden by reverse proxy</body></html>"
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403, content=body.encode(), headers={"content-type": "text/html"}
        )
    )
    with pytest.raises(AltegioBusinessError) as ei:
        await _do_book()
    assert ei.value.code == "unknown"
    assert ei.value.http_status == 403


@respx.mock
async def test_403_json_body_remains_business_error() -> None:
    """403 + application/json → existing business error parsing (не transport)."""
    body = {"errors": {"code": 403, "message": "Forbidden"}}
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403, json=body, headers={"content-type": "application/json"}
        )
    )
    with pytest.raises(AltegioBusinessError) as ei:
        await _do_book()
    assert ei.value.http_status == 403
    # Не cloudflare, не transport — остаёмся в business path.
    assert isinstance(ei.value, AltegioBusinessError)


@respx.mock
async def test_500_html_no_cloudflare_remains_transport() -> None:
    """500 + html → existing 5xx transport path. Cloudflare check не должен мешать."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            500,
            content=b"<html>Internal error</html>",
            headers={"content-type": "text/html"},
        )
    )
    with pytest.raises(AltegioTransportError) as ei:
        await _do_book()
    # Существующий 5xx path формирует cause "server error 500", НЕ "cloudflare_challenge".
    assert ei.value.cause == "server error 500"


@respx.mock
async def test_cloudflare_with_500_status_still_unknown() -> None:
    """500 + Cloudflare-подобный body → НЕ matched (требуем именно 403).

    Существующий 5xx-handler берёт верх, cause = "server error 500".
    """
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            500,
            content=PROD_CLOUDFLARE_BODY.encode(),
            headers={"content-type": "text/html"},
        )
    )
    with pytest.raises(AltegioTransportError) as ei:
        await _do_book()
    assert ei.value.cause != "cloudflare_challenge"
    assert ei.value.cause == "server error 500"


# ---- Logging ---------------------------------------------------------------


@respx.mock
async def test_cloudflare_logs_info_event(caplog: pytest.LogCaptureFixture) -> None:
    """INFO лог `altegio_cloudflare_challenge_detected` с body length, без raw body."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403,
            content=PROD_CLOUDFLARE_BODY.encode(),
            headers={"content-type": "text/html; charset=UTF-8"},
        )
    )
    caplog.set_level(logging.INFO, logger="tennis_booking.altegio.client")
    with pytest.raises(AltegioTransportError):
        await _do_book()

    info_msgs = [
        r.getMessage()
        for r in caplog.records
        if "altegio_cloudflare_challenge_detected" in r.getMessage()
    ]
    assert len(info_msgs) == 1
    assert "http_status=403" in info_msgs[0]
    assert "body_len=" in info_msgs[0]
    # Raw body НЕ должен попадать в лог.
    assert "Just a moment" not in info_msgs[0]
    assert "<title>" not in info_msgs[0]


@respx.mock
async def test_cloudflare_does_not_emit_unknown_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Cloudflare-ветка не должна засирать логи WARN'ом altegio_unknown_error_body."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403,
            content=PROD_CLOUDFLARE_BODY.encode(),
            headers={"content-type": "text/html"},
        )
    )
    caplog.set_level(logging.WARNING, logger="tennis_booking.altegio.client")
    with pytest.raises(AltegioTransportError):
        await _do_book()
    assert not any(
        "altegio_unknown_error_body" in r.getMessage() for r in caplog.records
    )


@respx.mock
async def test_unknown_403_html_still_emits_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Регрессия: 403 + html без cf-маркеров — existing WARN остаётся."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403,
            content=b"<html><body>Just a forbidden page</body></html>",
            headers={"content-type": "text/html"},
        )
    )
    caplog.set_level(logging.WARNING, logger="tennis_booking.altegio.client")
    with pytest.raises(AltegioBusinessError):
        await _do_book()
    assert any(
        "altegio_unknown_error_body" in r.getMessage() for r in caplog.records
    )
