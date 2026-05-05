from __future__ import annotations

import json
from datetime import UTC, datetime
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
    BookableStaff,
)
from tennis_booking.altegio.client import SEARCH_STAFF_PATH

BEARER = "test-bearer-secret"
COMPANY_ID = 521176
BOOKFORM_ID = 551098
BASE_URL = "https://b551098.alteg.io"
SERVICE_ID = 7849893
SLOT = datetime(2026, 5, 5, 22, 0, 0, tzinfo=ALMATY)


def _make_config(**overrides: Any) -> AltegioConfig:
    kwargs: dict[str, Any] = {
        "bearer_token": SecretStr(BEARER),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "bookform_id": BOOKFORM_ID,
    }
    kwargs.update(overrides)
    return AltegioConfig(**kwargs)


def _entry(staff_id: int, *, is_bookable: bool) -> dict[str, Any]:
    return {
        "type": "booking_search_result_staff",
        "id": str(staff_id),
        "attributes": {"is_bookable": is_bookable},
    }


# ---- Happy path ------------------------------------------------------------


@respx.mock
async def test_search_staff_at_datetime_success() -> None:
    body = {
        "data": [
            _entry(101, is_bookable=True),
            _entry(102, is_bookable=False),
            _entry(103, is_bookable=True),
            _entry(104, is_bookable=True),
            _entry(105, is_bookable=False),
            _entry(106, is_bookable=False),
            _entry(107, is_bookable=False),
        ]
    }
    route = respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200, json=body, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        entries = await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )

    assert len(entries) == 7
    assert all(isinstance(e, BookableStaff) for e in entries)
    bookable_ids = [e.staff_id for e in entries if e.is_bookable]
    assert bookable_ids == [101, 103, 104]

    request = route.calls.last.request
    assert request.method == "POST"
    assert request.url.path == SEARCH_STAFF_PATH
    assert request.headers["Authorization"] == f"Bearer {BEARER}"
    assert request.headers["Content-Type"] == "application/json"


@respx.mock
async def test_search_staff_at_datetime_body_shape() -> None:
    """Verify exact wire body — datetime ISO with TZ, staff_id null, service item."""
    route = respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200, json={"data": []}, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )

    sent_body = json.loads(route.calls.last.request.content)
    assert sent_body["context"]["location_id"] == COMPANY_ID
    assert sent_body["filter"]["datetime"] == "2026-05-05T22:00:00+05:00"
    records = sent_body["filter"]["records"]
    assert isinstance(records, list)
    assert len(records) == 1
    assert records[0]["staff_id"] is None
    assert records[0]["attendance_service_items"] == [
        {"type": "service", "id": SERVICE_ID}
    ]


@respx.mock
async def test_search_staff_at_datetime_empty_data_returns_empty_list() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200, json={"data": []}, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        entries = await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )
    assert entries == []


@respx.mock
async def test_search_staff_at_datetime_top_level_array_supported() -> None:
    body = [_entry(101, is_bookable=True)]
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200, json=body, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        entries = await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )
    assert len(entries) == 1
    assert entries[0].staff_id == 101


@respx.mock
async def test_search_staff_at_datetime_dry_run_noop() -> None:
    """Dry-run client returns empty without HTTP call (mutating-safe by default)."""
    route = respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}")
    async with AltegioClient(_make_config(dry_run=True)) as client:
        entries = await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )
    assert entries == []
    assert route.call_count == 0


# ---- Pre-flight validation -------------------------------------------------


@respx.mock
async def test_search_staff_at_datetime_pre_flight_naive_datetime_raises() -> None:
    """Asia/Almaty invariant: caller must pass tz-aware datetime."""
    route = respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}")
    naive = datetime(2026, 5, 5, 22, 0, 0)
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="timezone-aware"):
            await client.search_staff_at_datetime(
                datetime_local=naive, service_id=SERVICE_ID
            )
    assert route.call_count == 0


@respx.mock
async def test_search_staff_at_datetime_pre_flight_wrong_tz_raises() -> None:
    route = respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}")
    utc_dt = datetime(2026, 5, 5, 17, 0, 0, tzinfo=UTC)
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="Asia/Almaty"):
            await client.search_staff_at_datetime(
                datetime_local=utc_dt, service_id=SERVICE_ID
            )
    assert route.call_count == 0


@respx.mock
async def test_search_staff_at_datetime_pre_flight_bad_service_id_raises() -> None:
    route = respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="service_id"):
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=0
            )
    assert route.call_count == 0


# ---- Error response classification -----------------------------------------


@respx.mock
async def test_search_staff_at_datetime_4xx_raises_business_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            422,
            json={"meta": {"errors": [{"code": "bad_filter", "message": "x"}]}},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert ei.value.code == "bad_filter"
    assert ei.value.http_status == 422


@respx.mock
async def test_search_staff_at_datetime_5xx_raises_transport_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(503)
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert "503" in ei.value.cause


@respx.mock
async def test_search_staff_at_datetime_cloudflare_detection() -> None:
    cf_body = (
        "<html><head><title>Just a moment...</title></head>"
        "<body>challenges.cloudflare.com</body></html>"
    )
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            403,
            content=cf_body.encode(),
            headers={"content-type": "text/html"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert ei.value.cause == "cloudflare_challenge"


@respx.mock
async def test_search_staff_at_datetime_401_classified_as_unauthorized() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            401, content=b"", headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert ei.value.code == "unauthorized"


@respx.mock
async def test_search_staff_at_datetime_connect_error_transport() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        side_effect=httpx.ConnectError("nope")
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError):
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )


@respx.mock
async def test_search_staff_at_datetime_2xx_non_json_transport_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200, content=b"<html/>", headers={"content-type": "text/html"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert "non-JSON" in ei.value.cause


@respx.mock
async def test_search_staff_at_datetime_malformed_no_attributes() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"id": "101", "type": "x"}]},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_search_staff_at_datetime_malformed_is_bookable_not_bool() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "101",
                        "type": "x",
                        "attributes": {"is_bookable": "yes"},
                    }
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_search_staff_at_datetime_id_missing_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"type": "x", "attributes": {"is_bookable": True}}]},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_staff_at_datetime(
                datetime_local=SLOT, service_id=SERVICE_ID
            )
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_search_staff_at_datetime_id_as_int_supported() -> None:
    """Defensive: Altegio may send id as int instead of string."""
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {"id": 101, "type": "x", "attributes": {"is_bookable": True}}
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        entries = await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )
    assert len(entries) == 1
    assert entries[0].staff_id == 101


@respx.mock
async def test_search_staff_at_datetime_timeout_override_passed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_STAFF_PATH}").mock(
        return_value=httpx.Response(
            200, json={"data": []}, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        entries = await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID, timeout_s=0.2
        )
    assert entries == []


@respx.mock
async def test_search_staff_at_datetime_use_before_aenter_raises() -> None:
    client = AltegioClient(_make_config())
    with pytest.raises(RuntimeError, match="not entered"):
        await client.search_staff_at_datetime(
            datetime_local=SLOT, service_id=SERVICE_ID
        )
