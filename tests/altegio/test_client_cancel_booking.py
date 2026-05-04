from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest
import respx
from pydantic import SecretStr

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioClient,
    AltegioConfig,
    AltegioTransportError,
)
from tennis_booking.altegio.client import CANCEL_BOOKING_PATH

BEARER = "test-bearer-secret"
COMPANY_ID = 521176
BOOKFORM_ID = 551098
BASE_URL = "https://b551098.alteg.io"
RECORD_ID = 645847641
RECORD_HASH = "554f7a6a693197209816116ea42f3b09"
CANCEL_PATH = CANCEL_BOOKING_PATH.format(company_id=COMPANY_ID, record_id=RECORD_ID)


def _make_config(**overrides: Any) -> AltegioConfig:
    kwargs: dict[str, Any] = {
        "bearer_token": SecretStr(BEARER),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "bookform_id": BOOKFORM_ID,
    }
    kwargs.update(overrides)
    return AltegioConfig(**kwargs)


# ---- Happy path ------------------------------------------------------------


@respx.mock
async def test_cancel_booking_success_204() -> None:
    """204 No Content → no exception; method returns None."""
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(204)
    )
    async with AltegioClient(_make_config()) as client:
        result = await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert result is None
    assert route.call_count == 1


@respx.mock
async def test_cancel_booking_success_200() -> None:
    """200 OK with body should also be accepted."""
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"success": True},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert route.call_count == 1


# ---- URL / headers / query format ------------------------------------------


@respx.mock
async def test_cancel_booking_url_format() -> None:
    """Verify path includes record_id; query has hash and bookform_id; bearer set."""
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(204)
    )
    async with AltegioClient(_make_config()) as client:
        await client.cancel_booking(RECORD_ID, RECORD_HASH)

    request = route.calls.last.request
    assert request.method == "DELETE"
    assert request.url.path == CANCEL_PATH
    assert f"/attendances/{RECORD_ID}/" in request.url.path
    assert request.headers["Authorization"] == f"Bearer {BEARER}"

    # Query string contains both required params.
    query = parse_qs(request.url.query.decode())
    assert query["hash"] == [RECORD_HASH]
    assert query["bookform_id"] == [str(BOOKFORM_ID)]


# ---- Business errors -------------------------------------------------------


@respx.mock
async def test_cancel_booking_business_error_already_cancelled() -> None:
    """4xx with JSON body → AltegioBusinessError raised, code/message preserved."""
    error_body = {
        "meta": {
            "errors": [
                {"code": "already_cancelled", "message": "Booking already cancelled"}
            ]
        }
    }
    respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(
            422,
            json=error_body,
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as exc_info:
            await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert exc_info.value.http_status == 422
    assert exc_info.value.code == "already_cancelled"
    assert "already cancelled" in exc_info.value.message.lower()


@respx.mock
async def test_cancel_booking_401_maps_to_unauthorized() -> None:
    """401 with no recognisable code → mapped to 'unauthorized' (consistency
    with create_booking / search_timeslots paths)."""
    respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(
            401,
            text="<html>Unauthorized</html>",
            headers={"content-type": "text/html"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as exc_info:
            await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert exc_info.value.http_status == 401
    assert exc_info.value.code == "unauthorized"


# ---- Transport errors ------------------------------------------------------


@respx.mock
async def test_cancel_booking_transport_5xx() -> None:
    """5xx → AltegioTransportError with 'server error N' cause."""
    respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(503)
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as exc_info:
            await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert "503" in exc_info.value.cause


@respx.mock
async def test_cancel_booking_network_error() -> None:
    """httpx ConnectError → AltegioTransportError."""
    respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        side_effect=httpx.ConnectError("boom")
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as exc_info:
            await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert "ConnectError" in exc_info.value.cause


@respx.mock
async def test_cancel_booking_cloudflare_403_html_treated_as_transport() -> None:
    """403 + text/html + CF marker → treated as transport (CF challenge)."""
    cf_body = (
        "<html><head><title>Just a moment...</title></head>"
        "<body>challenges.cloudflare.com</body></html>"
    )
    respx.delete(f"{BASE_URL}{CANCEL_PATH}").mock(
        return_value=httpx.Response(
            403,
            text=cf_body,
            headers={"content-type": "text/html; charset=utf-8"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as exc_info:
            await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert exc_info.value.cause == "cloudflare_challenge"


# ---- Dry-run ---------------------------------------------------------------


@respx.mock
async def test_cancel_booking_dry_run_noop() -> None:
    """dry_run=True → no HTTP call, returns None silently."""
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}")
    async with AltegioClient(_make_config(dry_run=True)) as client:
        result = await client.cancel_booking(RECORD_ID, RECORD_HASH)

    assert result is None
    assert route.call_count == 0


# ---- Pre-flight validation -------------------------------------------------


@respx.mock
async def test_cancel_booking_validation_record_id_zero() -> None:
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="record_id"):
            await client.cancel_booking(0, RECORD_HASH)
    assert route.call_count == 0


@respx.mock
async def test_cancel_booking_validation_record_id_negative() -> None:
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="record_id"):
            await client.cancel_booking(-1, RECORD_HASH)
    assert route.call_count == 0


@respx.mock
async def test_cancel_booking_validation_record_id_not_int() -> None:
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="record_id"):
            await client.cancel_booking("abc", RECORD_HASH)  # type: ignore[arg-type]
    assert route.call_count == 0


@respx.mock
async def test_cancel_booking_validation_empty_record_hash() -> None:
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="record_hash"):
            await client.cancel_booking(RECORD_ID, "")
    assert route.call_count == 0


@respx.mock
async def test_cancel_booking_validation_whitespace_record_hash() -> None:
    route = respx.delete(f"{BASE_URL}{CANCEL_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="record_hash"):
            await client.cancel_booking(RECORD_ID, "   ")
    assert route.call_count == 0
