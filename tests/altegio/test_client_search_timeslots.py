from __future__ import annotations

import json
from datetime import date
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
    TimeSlot,
)
from tennis_booking.altegio.client import SEARCH_TIMESLOTS_PATH

BEARER = "test-bearer-secret"
COMPANY_ID = 521176
BOOKFORM_ID = 551098
BASE_URL = "https://b551098.alteg.io"
STAFF_ID = 1521566
DATE = date(2026, 4, 26)


def _make_config(**overrides: Any) -> AltegioConfig:
    kwargs: dict[str, Any] = {
        "bearer_token": SecretStr(BEARER),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "bookform_id": BOOKFORM_ID,
    }
    kwargs.update(overrides)
    return AltegioConfig(**kwargs)


def _slot(datetime_s: str, is_bookable: bool) -> dict[str, Any]:
    return {
        "type": "booking_search_result_timeslots",
        "id": "abc123",
        "attributes": {
            "datetime": datetime_s,
            "time": datetime_s[11:16],
            "is_bookable": is_bookable,
        },
    }


# ---- Happy path ------------------------------------------------------------


@respx.mock
async def test_search_timeslots_happy_path() -> None:
    body = {
        "data": [
            _slot("2026-04-26T22:00:00+05:00", False),
            _slot("2026-04-26T23:00:00+05:00", True),
        ]
    }
    route = respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, json=body, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[STAFF_ID]
        )
    assert len(slots) == 2
    assert all(isinstance(s, TimeSlot) for s in slots)
    assert slots[0].is_bookable is False
    assert slots[1].is_bookable is True
    # Datetime canonicalized to Almaty
    assert slots[1].dt.tzinfo is ALMATY
    assert slots[1].dt.hour == 23

    request = route.calls.last.request
    assert request.method == "POST"
    assert request.url.path == SEARCH_TIMESLOTS_PATH
    assert request.headers["Authorization"] == f"Bearer {BEARER}"
    assert request.headers["Content-Type"] == "application/json"
    sent_body = json.loads(request.content)
    assert sent_body["context"]["location_id"] == COMPANY_ID
    assert sent_body["filter"]["date"] == "2026-04-26"
    assert sent_body["filter"]["records"] == [
        {"staff_id": STAFF_ID, "attendance_service_items": []}
    ]


@respx.mock
async def test_search_timeslots_multiple_staff_ids_in_body() -> None:
    body = {"data": []}
    route = respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, json=body, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[1, 2, 3]
        )
    assert slots == []
    sent_body = json.loads(route.calls.last.request.content)
    assert sent_body["filter"]["records"] == [
        {"staff_id": 1, "attendance_service_items": []},
        {"staff_id": 2, "attendance_service_items": []},
        {"staff_id": 3, "attendance_service_items": []},
    ]


@respx.mock
async def test_search_timeslots_empty_data_returns_empty_list() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": []},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[STAFF_ID]
        )
    assert slots == []


@respx.mock
async def test_search_timeslots_top_level_array_supported() -> None:
    """Defensive: if Altegio ever returns a bare array instead of {data: [...]}."""
    body = [_slot("2026-04-26T23:00:00+05:00", True)]
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, json=body, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[STAFF_ID]
        )
    assert len(slots) == 1


@respx.mock
async def test_search_timeslots_dry_run_still_posts() -> None:
    """Dry-run does NOT skip search_timeslots — it's read-only and safe."""
    route = respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": []},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config(dry_run=True)) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[STAFF_ID]
        )
    assert slots == []
    assert route.call_count == 1


# ---- Pre-flight validation -------------------------------------------------


@respx.mock
async def test_empty_staff_ids_rejected_no_post() -> None:
    route = respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="staff_ids"):
            await client.search_timeslots(date_local=DATE, staff_ids=[])
    assert route.call_count == 0


@respx.mock
async def test_duplicate_staff_ids_rejected_no_post() -> None:
    route = respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="unique"):
            await client.search_timeslots(date_local=DATE, staff_ids=[1, 1, 2])
    assert route.call_count == 0


@respx.mock
async def test_zero_staff_id_rejected() -> None:
    route = respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match=">="):
            await client.search_timeslots(date_local=DATE, staff_ids=[0])
    assert route.call_count == 0


# ---- Error response classification -----------------------------------------


@respx.mock
async def test_5xx_transport_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(500)
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError):
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])


@respx.mock
async def test_4xx_business_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            422,
            json={"meta": {"errors": [{"code": "bad_filter", "message": "x"}]}},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert ei.value.code == "bad_filter"
    assert ei.value.http_status == 422


@respx.mock
async def test_401_classified_as_unauthorized() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            401, content=b"", headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert ei.value.code == "unauthorized"


@respx.mock
async def test_connect_error_transport() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        side_effect=httpx.ConnectError("nope")
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError):
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])


@respx.mock
async def test_2xx_non_json_transport_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, content=b"<html/>", headers={"content-type": "text/html"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "non-JSON" in ei.value.cause


@respx.mock
async def test_2xx_invalid_json_transport_error() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            content=b"not json",
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "invalid JSON" in ei.value.cause


@respx.mock
async def test_2xx_malformed_data_not_a_list() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": {"oops": "not a list"}},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_2xx_slot_without_attributes_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"id": "x", "type": "y"}]},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_2xx_naive_datetime_rejected_as_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "type": "ts",
                        "id": "x",
                        "attributes": {
                            "datetime": "2026-04-26T23:00:00",
                            "is_bookable": True,
                        },
                    }
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "tz-aware" in ei.value.message


@respx.mock
async def test_timeout_override_passed_to_request() -> None:
    """Verify timeout_s parameter is accepted (and request still passes)."""
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": []},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[STAFF_ID], timeout_s=2.5
        )
    assert slots == []


@respx.mock
async def test_3xx_unexpected_transport() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(301, headers={"location": "/elsewhere"})
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError):
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])


@respx.mock
async def test_use_before_aenter_raises() -> None:
    client = AltegioClient(_make_config())
    with pytest.raises(RuntimeError, match="not entered"):
        await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])


@respx.mock
async def test_2xx_data_none_returns_empty_list() -> None:
    """If `data` key is missing entirely, treat as empty result."""
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, json={"meta": {}}, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert slots == []


@respx.mock
async def test_2xx_data_item_not_mapping_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": ["not a dict"]},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_2xx_top_level_string_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, json="just a string", headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_2xx_datetime_missing_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={"data": [{"type": "x", "id": "y", "attributes": {"is_bookable": True}}]},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "datetime" in ei.value.message


@respx.mock
async def test_2xx_datetime_not_iso_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "type": "x",
                        "id": "y",
                        "attributes": {
                            "datetime": "not-a-datetime",
                            "is_bookable": True,
                        },
                    }
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "ISO-8601" in ei.value.message


@respx.mock
async def test_2xx_is_bookable_not_bool_malformed() -> None:
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "type": "x",
                        "id": "y",
                        "attributes": {
                            "datetime": "2026-04-26T23:00:00+05:00",
                            "is_bookable": "yes",
                        },
                    }
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "is_bookable" in ei.value.message


@respx.mock
async def test_2xx_staff_id_non_int_silently_dropped() -> None:
    """If staff_id is malformed (string/bool), treat as None — best effort, not fatal."""
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "type": "x",
                        "id": "y",
                        "attributes": {
                            "datetime": "2026-04-26T23:00:00+05:00",
                            "is_bookable": True,
                            "staff_id": "1521567",  # string, not int
                        },
                    }
                ]
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert len(slots) == 1
    assert slots[0].staff_id is None


@respx.mock
async def test_unexpected_exception_wrapped_as_transport() -> None:
    """RuntimeError or similar non-httpx error from network → wrapped as transport."""
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        side_effect=RuntimeError("weird")
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.search_timeslots(date_local=DATE, staff_ids=[STAFF_ID])
    assert "RuntimeError" in ei.value.cause


@respx.mock
async def test_search_timeslots_with_staff_id_attribute() -> None:
    """If response includes per-slot staff_id, parser captures it."""
    body = {
        "data": [
            {
                "type": "ts",
                "id": "x",
                "attributes": {
                    "datetime": "2026-04-26T23:00:00+05:00",
                    "is_bookable": True,
                    "staff_id": 1521567,
                },
            }
        ]
    }
    respx.post(f"{BASE_URL}{SEARCH_TIMESLOTS_PATH}").mock(
        return_value=httpx.Response(
            200, json=body, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        slots = await client.search_timeslots(
            date_local=DATE, staff_ids=[1521566, 1521567]
        )
    assert len(slots) == 1
    assert slots[0].staff_id == 1521567
