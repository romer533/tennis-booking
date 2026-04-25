from __future__ import annotations

import asyncio
import json
import logging
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
    BookingResponse,
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

SUCCESS_BODY = [
    {
        "id": 0,
        "record_id": 645268016,
        "record_hash": "554f7a6a693197209816116ea42f3b09",
        "record": {"id": 645268016},
    }
]


def _make_config(**overrides: Any) -> AltegioConfig:
    kwargs: dict[str, Any] = {
        "bearer_token": SecretStr(BEARER),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "bookform_id": BOOKFORM_ID,
    }
    kwargs.update(overrides)
    return AltegioConfig(**kwargs)


# ---- Happy path / wire format ----------------------------------------------


@respx.mock
async def test_create_booking_happy_path() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201,
            json=SUCCESS_BODY,
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        resp = await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="Roman",
            phone="77026473809",
        )

    assert isinstance(resp, BookingResponse)
    assert resp.record_id == 645268016
    assert resp.record_hash == "554f7a6a693197209816116ea42f3b09"

    assert route.call_count == 1
    request = route.calls.last.request
    assert request.method == "POST"
    assert request.url.path == BOOK_PATH
    assert request.headers["Authorization"] == f"Bearer {BEARER}"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["accept"] == "application/json, text/plain, */*"
    body = json.loads(request.content)
    assert body["fullname"] == "Roman"
    assert body["phone"] == "77026473809"
    assert body["bookform_id"] == BOOKFORM_ID
    assert body["appointments"][0]["staff_id"] == STAFF_ID
    assert body["appointments"][0]["services"] == [SERVICE_ID]
    assert body["appointments"][0]["datetime"] == "2026-04-26T23:00:00"
    assert body["appointments"][0]["available_staff_ids"] == [STAFF_ID]
    assert body["notify_by_sms"] == 1
    assert body["is_charge_required_priority"] is True


@respx.mock
async def test_datetime_is_naive_local_in_wire() -> None:
    """slot_dt_local в Almaty → wire `2026-04-26T23:00:00` без TZ-суффикса."""
    route = respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY),
            fullname="X",
            phone="7",
        )
    body = json.loads(route.calls.last.request.content)
    assert body["appointments"][0]["datetime"] == "2026-04-26T23:00:00"
    assert "+05:00" not in body["appointments"][0]["datetime"]
    assert "Z" not in body["appointments"][0]["datetime"]


@respx.mock
async def test_email_none_omitted_from_body() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="X",
            phone="7",
            email=None,
        )
    body = json.loads(route.calls.last.request.content)
    assert "email" not in body


@respx.mock
async def test_email_present_in_body() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="X",
            phone="7",
            email="user@example.com",
        )
    body = json.loads(route.calls.last.request.content)
    assert body["email"] == "user@example.com"


# ---- Pre-flight validation --------------------------------------------------


@respx.mock
async def test_naive_datetime_rejected_no_post() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="timezone-aware"):
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=datetime(2026, 4, 26, 23, 0, 0),
                fullname="X",
                phone="7",
            )
    assert route.call_count == 0


@respx.mock
async def test_utc_datetime_rejected_no_post() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="Asia/Almaty"):
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=datetime(2026, 4, 26, 23, 0, 0, tzinfo=UTC),
                fullname="X",
                phone="7",
            )
    assert route.call_count == 0


@respx.mock
async def test_empty_fullname_rejected_no_post() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="fullname"):
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="   ",
                phone="7",
            )
    assert route.call_count == 0


@respx.mock
async def test_empty_phone_rejected_no_post() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="phone"):
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="",
            )
    assert route.call_count == 0


@respx.mock
async def test_zero_service_id_rejected() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="service_id"):
            await client.create_booking(
                service_id=0,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert route.call_count == 0


@respx.mock
async def test_negative_staff_id_rejected() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(ValueError, match="staff_id"):
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=-1,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert route.call_count == 0


# ---- Error response classification -----------------------------------------


@respx.mock
async def test_4xx_legacy_meta_errors_array_regression() -> None:
    """P2 regression: existing meta.errors[] shape must keep working — parser
    update must NOT break the old contract."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            422,
            json={
                "meta": {
                    "errors": [
                        {"code": "any_legacy_code", "message": "legacy text"}
                    ]
                }
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "any_legacy_code"
    assert ei.value.message == "legacy text"
    assert ei.value.http_status == 422


@respx.mock
async def test_4xx_with_meta_errors_business_error() -> None:
    # TODO(provocation #1/#2): verify actual Altegio 4xx shape after manual provocation.
    # Current shape {"meta":{"errors":[...]}} is hypothetical, based on general Altegio API conventions.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            422,
            json={
                "meta": {
                    "errors": [
                        {"code": "slot_busy", "message": "Слот уже занят"}
                    ]
                }
            },
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "slot_busy"
    assert ei.value.message == "Слот уже занят"
    assert ei.value.http_status == 422


@respx.mock
async def test_4xx_with_meta_message_only() -> None:
    # TODO(provocation #1/#2): verify actual Altegio 4xx shape after manual provocation.
    # Current shape {"meta":{"message":...},"success":false} is hypothetical, based on general Altegio API conventions.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            400,
            json={"meta": {"message": "bad input"}, "success": False},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "unknown"
    assert ei.value.message == "bad input"
    assert ei.value.http_status == 400


@respx.mock
async def test_4xx_empty_body_unknown_code() -> None:
    # TODO(provocation #1/#2): verify actual Altegio 4xx shape after manual provocation.
    # Empty-body 4xx is a hypothetical fallback path; real Altegio errors may always carry a body.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            400, content=b"", headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "unknown"
    assert ei.value.message == "<empty>"
    assert ei.value.http_status == 400


@respx.mock
async def test_4xx_html_body_business_error() -> None:
    """Чёткое решение: 4xx HTML — серверная классификация ошибки → Business, не Transport."""
    # TODO(provocation #1/#2): verify actual Altegio 4xx shape after manual provocation.
    # 4xx with HTML body is a hypothetical edge case (e.g., upstream proxy/WAF); real Altegio shape unconfirmed.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            403,
            content=b"<html><body>Forbidden</body></html>",
            headers={"content-type": "text/html"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "unknown"
    assert "Forbidden" in ei.value.message
    assert ei.value.http_status == 403


@respx.mock
async def test_401_classified_as_unauthorized() -> None:
    # TODO(provocation #3): verify actual Altegio 401 shape after manual provocation
    # (e.g., expired/invalid Bearer token). Empty-body 401 is the hypothetical worst case.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            401, content=b"", headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "unauthorized"
    assert ei.value.http_status == 401


@respx.mock
async def test_401_with_explicit_code_kept() -> None:
    """Если сервер прислал свой code на 401 — его и используем (не перетираем 'unauthorized')."""
    # TODO(provocation #3): verify actual Altegio 401 shape after manual provocation.
    # The "token_expired" code is hypothetical, based on general Altegio API conventions.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            401,
            json={"meta": {"errors": [{"code": "token_expired", "message": "x"}]}},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "token_expired"


@respx.mock
async def test_5xx_transport_error() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(503, content=b"unavailable")
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "503" in ei.value.cause


@respx.mock
async def test_2xx_non_json_transport_error() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            200, content=b"<html>...</html>", headers={"content-type": "text/html"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "non-JSON" in ei.value.cause


@respx.mock
async def test_2xx_json_missing_record_id_malformed() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201,
            json=[{"record_hash": "h"}],
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_2xx_empty_array_malformed() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=[], headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_2xx_dict_response_accepted() -> None:
    """На случай, если Altegio когда-нибудь поменяет shape с array на dict — best effort."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201,
            json={"record_id": 42, "record_hash": "h"},
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        resp = await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="X",
            phone="7",
        )
    assert resp.record_id == 42


# ---- Network-level errors ---------------------------------------------------


@respx.mock
async def test_connect_error_transport() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(side_effect=httpx.ConnectError("nope"))
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "ConnectError" in ei.value.cause


@respx.mock
async def test_read_timeout_transport() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(side_effect=httpx.ReadTimeout("slow"))
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "ReadTimeout" in ei.value.cause


@respx.mock
async def test_connect_timeout_transport() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(side_effect=httpx.ConnectTimeout("ct"))
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "ConnectTimeout" in ei.value.cause


@respx.mock
async def test_remote_protocol_error_transport() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        side_effect=httpx.RemoteProtocolError("rpe")
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "RemoteProtocolError" in ei.value.cause


# ---- prearm -----------------------------------------------------------------


@respx.mock
async def test_prearm_calls_get_root() -> None:
    route = respx.get(f"{BASE_URL}/").mock(
        return_value=httpx.Response(404, content=b"not found")
    )
    async with AltegioClient(_make_config()) as client:
        await client.prearm()
    assert route.call_count == 1


@respx.mock
async def test_prearm_idempotent() -> None:
    route = respx.get(f"{BASE_URL}/").mock(return_value=httpx.Response(200))
    async with AltegioClient(_make_config()) as client:
        await client.prearm()
        await client.prearm()
    assert route.call_count == 2


@respx.mock
async def test_prearm_dry_run_skipped() -> None:
    route = respx.get(f"{BASE_URL}/")
    async with AltegioClient(_make_config(dry_run=True)) as client:
        await client.prearm()
    assert route.call_count == 0


@respx.mock
async def test_prearm_connect_error_raises_transport() -> None:
    respx.get(f"{BASE_URL}/").mock(side_effect=httpx.ConnectError("nope"))
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError):
            await client.prearm()


# ---- dry-run ----------------------------------------------------------------


@respx.mock
async def test_create_booking_dry_run_no_post() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config(dry_run=True)) as client:
        resp = await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="X",
            phone="7",
        )
    assert route.call_count == 0
    assert resp.record_id == 0
    assert resp.record_hash == "dry-run"


@respx.mock
async def test_create_booking_dry_run_still_validates_inputs() -> None:
    route = respx.post(f"{BASE_URL}{BOOK_PATH}")
    async with AltegioClient(_make_config(dry_run=True)) as client:
        with pytest.raises(ValueError, match="timezone-aware"):
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=datetime(2026, 4, 26, 23, 0, 0),
                fullname="X",
                phone="7",
            )
    assert route.call_count == 0


# ---- Concurrency ------------------------------------------------------------


@respx.mock
async def test_parallel_bookings_no_race() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        results = await asyncio.gather(
            *[
                client.create_booking(
                    service_id=SERVICE_ID,
                    staff_id=STAFF_ID,
                    slot_dt_local=SLOT,
                    fullname=f"User-{i}",
                    phone="7",
                )
                for i in range(3)
            ]
        )
    assert all(r.record_id == 645268016 for r in results)
    assert len(results) == 3


# ---- Bearer masking ---------------------------------------------------------


@respx.mock
async def test_bearer_not_in_caplog(caplog: pytest.LogCaptureFixture) -> None:
    """Гарантия: ни один формат логирования httpx не утечёт Bearer."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")
    caplog.set_level(logging.DEBUG, logger="tennis_booking.altegio")

    async with AltegioClient(_make_config()) as client:
        await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="X",
            phone="7",
        )

    for record in caplog.records:
        msg = record.getMessage()
        assert BEARER not in msg, f"bearer leaked into log: {msg!r}"


def test_bearer_not_in_caplog_httpcore(caplog: pytest.LogCaptureFixture) -> None:
    """httpcore.* loggers тоже должны быть зачищены (TRACE-уровень логирует raw bytes с Authorization)."""
    from tennis_booking.altegio.client import _BearerRedactFilter

    # Эмулируем сценарий: httpcore.http11 логирует строку с Bearer на уровне DEBUG.
    # Filter, повешенный на этот logger при импорте client, должен подменить токен.
    httpcore_logger = logging.getLogger("httpcore.http11")
    assert any(isinstance(f, _BearerRedactFilter) for f in httpcore_logger.filters), (
        "_BearerRedactFilter must be installed on httpcore.http11"
    )

    caplog.set_level(logging.DEBUG, logger="httpcore.http11")
    httpcore_logger.debug(
        "send_request_headers.complete return_value=[(b'Authorization', b'Bearer %s')]",
        BEARER,
    )

    for record in caplog.records:
        msg = record.getMessage()
        assert BEARER not in msg, f"bearer leaked into httpcore log: {msg!r}"


def test_repr_does_not_contain_bearer() -> None:
    client = AltegioClient(_make_config())
    assert BEARER not in repr(client)


def test_config_property_exposes_config() -> None:
    cfg = _make_config()
    client = AltegioClient(cfg)
    assert client.config is cfg


@respx.mock
async def test_prearm_pool_timeout_raises_transport() -> None:
    """Покрываем общий httpx.TransportError путь (не специально перехваченный subclass)."""
    respx.get(f"{BASE_URL}/").mock(side_effect=httpx.PoolTimeout("pool"))
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError):
            await client.prearm()


def test_business_error_str_does_not_contain_bearer() -> None:
    err = AltegioBusinessError(code="slot_busy", message="not your token", http_status=422)
    assert BEARER not in str(err)


# ---- Lifecycle --------------------------------------------------------------


async def test_use_after_aexit_raises() -> None:
    client = AltegioClient(_make_config())
    async with client:
        pass
    with pytest.raises(RuntimeError):
        await client.prearm()


async def test_use_before_aenter_raises() -> None:
    client = AltegioClient(_make_config())
    with pytest.raises(RuntimeError, match="not entered"):
        await client.prearm()


@respx.mock
async def test_dry_run_works_before_aenter() -> None:
    """dry_run.create_booking даже без http-клиента возвращает sentinel — без сетевых вызовов."""
    client = AltegioClient(_make_config(dry_run=True))
    resp = await client.create_booking(
        service_id=SERVICE_ID,
        staff_id=STAFF_ID,
        slot_dt_local=SLOT,
        fullname="X",
        phone="7",
    )
    assert resp.record_hash == "dry-run"


# ---- External http override -------------------------------------------------


@respx.mock
async def test_external_http_not_closed_on_aexit() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    external = httpx.AsyncClient(base_url=BASE_URL)
    try:
        async with AltegioClient(_make_config(), http=external) as client:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
        assert not external.is_closed
    finally:
        await external.aclose()


# ---- timeout override -------------------------------------------------------


@respx.mock
async def test_3xx_unexpected_transport() -> None:
    """3xx — это редирект; логически не наша история, классифицируем как transport."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(301, headers={"location": "/elsewhere"})
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "301" in ei.value.cause


@respx.mock
async def test_2xx_invalid_json_transport() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201,
            content=b"not valid json {",
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "invalid JSON" in ei.value.cause


@respx.mock
async def test_2xx_top_level_string_malformed() -> None:
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json="just a string", headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "malformed_success"


@respx.mock
async def test_4xx_invalid_json_falls_back_to_text() -> None:
    """JSON content-type, но body не валидный JSON — fallback в truncated raw text."""
    # TODO(provocation #1/#2): verify actual Altegio 4xx shape after manual provocation.
    # JSON content-type with non-JSON body is a hypothetical defensive case; real Altegio body shape unconfirmed.
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            422,
            content=b"not json",
            headers={"content-type": "application/json"},
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert ei.value.code == "unknown"
    assert "not json" in ei.value.message


@respx.mock
async def test_4xx_long_html_truncated() -> None:
    # TODO(provocation #1/#2): verify actual Altegio 4xx shape after manual provocation.
    # Large HTML 4xx body is a hypothetical defensive case (e.g., upstream WAF page); real Altegio shape unconfirmed.
    long_body = b"X" * 1500
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            418, content=long_body, headers={"content-type": "text/html"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioBusinessError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "truncated" in ei.value.message
    assert "total=1500" in ei.value.message


@respx.mock
async def test_unexpected_exception_wrapped_as_transport() -> None:
    """Ловушка для редких httpx-ошибок не из иерархии TransportError (например, OSError)."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(side_effect=RuntimeError("weird"))
    async with AltegioClient(_make_config()) as client:
        with pytest.raises(AltegioTransportError) as ei:
            await client.create_booking(
                service_id=SERVICE_ID,
                staff_id=STAFF_ID,
                slot_dt_local=SLOT,
                fullname="X",
                phone="7",
            )
    assert "RuntimeError" in ei.value.cause


async def test_reentering_closed_client_raises() -> None:
    client = AltegioClient(_make_config())
    async with client:
        pass
    with pytest.raises(RuntimeError, match="already closed"):
        async with client:
            pass


def test_bearer_redact_filter_handles_broken_record(caplog: pytest.LogCaptureFixture) -> None:
    """Filter не должен падать на record, у которого getMessage() кидает."""
    from tennis_booking.altegio.client import _BearerRedactFilter

    flt = _BearerRedactFilter()

    class _BrokenRecord:
        def getMessage(self) -> str:  # noqa: N802 — emulate logging.LogRecord API
            raise ValueError("boom")

    assert flt.filter(_BrokenRecord()) is True  # type: ignore[arg-type]


def test_install_bearer_filter_idempotent() -> None:
    from tennis_booking.altegio.client import (
        _BearerRedactFilter,
        _install_bearer_filter,
    )

    httpx_logger = logging.getLogger("httpx")
    before = sum(1 for f in httpx_logger.filters if isinstance(f, _BearerRedactFilter))
    _install_bearer_filter()
    _install_bearer_filter()
    after = sum(1 for f in httpx_logger.filters if isinstance(f, _BearerRedactFilter))
    assert before == after


def test_bearer_redact_filter_redacts() -> None:
    from tennis_booking.altegio.client import _BearerRedactFilter

    flt = _BearerRedactFilter()
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname="x",
        lineno=1,
        msg="POST with header Authorization: Bearer abc.def.ghi-XYZ",
        args=None,
        exc_info=None,
    )
    flt.filter(record)
    assert "abc.def.ghi-XYZ" not in record.getMessage()
    assert "Bearer ***" in record.getMessage()


@respx.mock
async def test_timeout_override_passed_to_request() -> None:
    """Хот-path может передать кастомный таймаут — убедимся, что не падает."""
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            201, json=SUCCESS_BODY, headers={"content-type": "application/json"}
        )
    )
    async with AltegioClient(_make_config()) as client:
        resp = await client.create_booking(
            service_id=SERVICE_ID,
            staff_id=STAFF_ID,
            slot_dt_local=SLOT,
            fullname="X",
            phone="7",
            timeout_s=0.5,
        )
    assert resp.record_id == 645268016
