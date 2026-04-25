"""Тесты `_extract_business_error` на новый shape Altegio.

Production incident 24.04 02:00 UTC: Altegio для 422 при недоступном слоте
возвращает {"errors": {"code": 422, "message": "..."}, "meta": {"message": "..."}}.
Старый парсер искал `meta.errors[0].code` (массив) → не находил → fallback "unknown"
→ engine fallback "lost", без retry.
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
)
from tennis_booking.altegio.client import (
    BOOK_RECORD_PATH,
    _extract_business_error,
)

BEARER = "test-bearer-secret"
COMPANY_ID = 521176
BOOKFORM_ID = 551098
BASE_URL = "https://b551098.alteg.io"
SERVICE_ID = 7849893
STAFF_ID = 1521566
SLOT = datetime(2026, 4, 26, 23, 0, 0, tzinfo=ALMATY)
BOOK_PATH = BOOK_RECORD_PATH.format(company_id=COMPANY_ID)


def _make_config(**overrides: Any) -> AltegioConfig:
    kwargs: dict[str, Any] = {
        "bearer_token": SecretStr(BEARER),
        "base_url": BASE_URL,
        "company_id": COMPANY_ID,
        "bookform_id": BOOKFORM_ID,
    }
    kwargs.update(overrides)
    return AltegioConfig(**kwargs)


def _resp(status: int, body: Any, *, content_type: str = "application/json") -> httpx.Response:
    return httpx.Response(
        status_code=status,
        json=body if isinstance(body, (dict, list, str, int, float, bool, type(None))) else None,
        content=body if isinstance(body, bytes) else None,
        headers={"content-type": content_type},
    )


# ---- Direct unit tests on _extract_business_error --------------------------


class TestPrecedence:
    """Order: meta.errors[] (legacy) > errors{} (new) > meta.message > unknown."""

    def test_legacy_meta_errors_array_wins_over_top_errors(self) -> None:
        body = {
            "meta": {"errors": [{"code": "legacy_code", "message": "legacy"}]},
            "errors": {"code": 422, "message": "service is not available"},
        }
        resp = _resp(422, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "legacy_code"
        assert message == "legacy"

    def test_legacy_meta_errors_array_wins_over_meta_message(self) -> None:
        body = {
            "meta": {
                "errors": [{"code": "legacy", "message": "from-array"}],
                "message": "service is not available",
            }
        }
        resp = _resp(422, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "legacy"
        assert message == "from-array"

    def test_top_errors_wins_over_meta_message(self) -> None:
        body = {
            "errors": {"code": 422, "message": "service is not available bla"},
            "meta": {"message": "unauthorized"},
        }
        resp = _resp(422, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "service_not_available"
        assert message == "service is not available bla"


class TestNewTopErrorsShape:
    """New shape: {"errors": {"code": ..., "message": "..."}} (production incident)."""

    def test_production_incident_shape(self) -> None:
        """Реальное body из production incident 24.04 02:00 UTC."""
        body = {
            "errors": {
                "code": 422,
                "message": "The service is not available at the selected time. Please choose a different time.",
            },
            "meta": {
                "message": "The service is not available at the selected time. Please choose a different time."
            },
        }
        resp = _resp(422, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "service_not_available"
        assert "service is not available" in message.lower()

    def test_top_errors_string_code_ignored_for_text_mapping(self) -> None:
        """Code derived from text, not from numeric code field."""
        body = {"errors": {"code": "weird_string", "message": "service is not available"}}
        resp = _resp(422, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "service_not_available"

    def test_top_errors_unauthorized_text(self) -> None:
        body = {"errors": {"code": 401, "message": "Unauthorized: bearer expired"}}
        resp = _resp(401, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "unauthorized"

    def test_top_errors_unknown_text(self) -> None:
        body = {"errors": {"code": 422, "message": "Some random thing happened"}}
        resp = _resp(422, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "unknown"
        assert message == "Some random thing happened"

    def test_top_errors_empty_message_falls_through(self) -> None:
        """Если message пуст — top_errors не сработал, идём дальше (meta.message или unknown)."""
        body = {"errors": {"code": 422, "message": ""}}
        resp = _resp(422, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "unknown"

    def test_top_errors_missing_message_falls_through(self) -> None:
        body = {"errors": {"code": 422}}
        resp = _resp(422, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "unknown"

    def test_top_errors_not_a_dict_ignored(self) -> None:
        """Если errors — список (not dict), top_errors не срабатывает."""
        body = {"errors": [{"code": 1, "message": "x"}]}
        resp = _resp(422, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "unknown"


class TestMetaMessageShape:
    """meta.message-only shape (existing behaviour with new derive)."""

    def test_meta_message_service_not_available(self) -> None:
        body = {"meta": {"message": "The service is not available at this time"}}
        resp = _resp(422, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "service_not_available"

    def test_meta_message_unauthorized(self) -> None:
        body = {"meta": {"message": "Unauthorized"}}
        resp = _resp(401, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "unauthorized"

    def test_meta_message_unknown(self) -> None:
        body = {"meta": {"message": "bad input"}}
        resp = _resp(400, body)
        code, message = _extract_business_error(resp, is_json=True)
        assert code == "unknown"
        assert message == "bad input"


class TestTextMappingCaseInsensitive:
    def test_uppercase_service_not_available(self) -> None:
        body = {"errors": {"code": 422, "message": "SERVICE IS NOT AVAILABLE"}}
        resp = _resp(422, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "service_not_available"

    def test_mixed_case_unauthorized(self) -> None:
        body = {"errors": {"code": 401, "message": "UnAuThOrIzEd"}}
        resp = _resp(401, body)
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "unauthorized"


class TestUnknownLogging:
    def test_unknown_logs_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        body = {"unrecognized": "shape"}
        resp = _resp(422, body)
        caplog.set_level(logging.WARNING, logger="tennis_booking.altegio.client")
        code, _ = _extract_business_error(resp, is_json=True)
        assert code == "unknown"
        assert any(
            "altegio_unknown_error_body" in r.getMessage()
            for r in caplog.records
        )

    def test_known_does_not_log_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        body = {"errors": {"code": 422, "message": "service is not available"}}
        resp = _resp(422, body)
        caplog.set_level(logging.WARNING, logger="tennis_booking.altegio.client")
        _extract_business_error(resp, is_json=True)
        assert not any(
            "altegio_unknown_error_body" in r.getMessage()
            for r in caplog.records
        )

    def test_legacy_known_does_not_log_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        body = {"meta": {"errors": [{"code": "any_code", "message": "anything"}]}}
        resp = _resp(422, body)
        caplog.set_level(logging.WARNING, logger="tennis_booking.altegio.client")
        _extract_business_error(resp, is_json=True)
        assert not any(
            "altegio_unknown_error_body" in r.getMessage()
            for r in caplog.records
        )


# ---- N2 production regression: full HTTP roundtrip via respx --------------


@respx.mock
async def test_n2_production_regression_422_4_shots() -> None:
    """N2 regression: реальный prod body 422 → engine treats as service_not_available
    → not_open retries → timeout='not_open_deadline'. На СТАРОМ коде падает.

    Здесь — низкоуровневый тест на altegio_client+engine fallback, а engine grace-test
    в test_attempt_grace_polling.py делает аналог с grace_polling=None.
    """

    from datetime import UTC, timedelta

    from tennis_booking.engine.attempt import AttemptConfig, BookingAttempt
    from tests.engine.conftest import FakeClock, as_clock

    prod_body = {
        "errors": {
            "code": 422,
            "message": "The service is not available at the selected time. Please choose a different time.",
        },
        "meta": {
            "message": "The service is not available at the selected time. Please choose a different time."
        },
    }
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            422, json=prod_body, headers={"content-type": "application/json"}
        )
    )

    clock = FakeClock(initial_utc=datetime(2026, 4, 23, 2, 0, 0, tzinfo=UTC))
    cfg = AttemptConfig(
        slot_dt_local=SLOT,
        court_ids=(STAFF_ID, STAFF_ID + 1, STAFF_ID + 2, STAFF_ID + 3),
        service_id=SERVICE_ID,
        fullname="Roman",
        phone="77000",
        profile_name="roman",
        not_open_retry_ms=100,
        not_open_deadline_s=1.0,
        global_deadline_s=2.0,
        prearm_lead_s=30.0,
        grace_polling=None,
    )
    window = clock.now_utc() + timedelta(seconds=60)

    async with AltegioClient(_make_config()) as client:
        attempt = BookingAttempt(cfg, client, as_clock(clock))
        result = await attempt.run(window)

    assert result.status == "timeout"
    assert result.business_code == "service_not_available"


@respx.mock
async def test_n2_business_error_raised_with_correct_code() -> None:
    """422 prod body → AltegioBusinessError(code='service_not_available')."""
    prod_body = {
        "errors": {
            "code": 422,
            "message": "The service is not available at the selected time. Please choose a different time.",
        },
        "meta": {
            "message": "The service is not available at the selected time. Please choose a different time."
        },
    }
    respx.post(f"{BASE_URL}{BOOK_PATH}").mock(
        return_value=httpx.Response(
            422, json=prod_body, headers={"content-type": "application/json"}
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
    assert ei.value.code == "service_not_available"
    assert ei.value.http_status == 422
    assert "service is not available" in ei.value.message.lower()
