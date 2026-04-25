from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime
from types import TracebackType
from typing import Any

import httpx
from pydantic import ValidationError

from tennis_booking.common.tz import ALMATY

from .config import AltegioConfig
from .errors import AltegioBusinessError, AltegioTransportError
from .models import BookingAppointment, BookingRequest, BookingResponse, TimeSlot

__all__ = ["ALMATY", "BOOK_RECORD_PATH", "SEARCH_TIMESLOTS_PATH", "AltegioClient"]

BOOK_RECORD_PATH = "/api/v1/book_record/{company_id}"
SEARCH_TIMESLOTS_PATH = "/api/v1/booking/search/timeslots/"

_DEFAULT_TIMEOUT = httpx.Timeout(connect=2.0, read=5.0, write=5.0, pool=2.0)
_DEFAULT_LIMITS = httpx.Limits(max_connections=10, max_keepalive_connections=5)

_MAX_ERROR_BODY_CHARS = 500

_logger = logging.getLogger(__name__)

# Global filter on httpx + httpcore loggers — strips Bearer tokens from formatted records.
# httpcore вызывает trace-logging на уровне raw bytes (HTTPCORE_LOG_LEVEL=trace) — там тоже может всплыть header.
_BEARER_RE = re.compile(r"(Bearer\s+)([^\s'\"]+)", re.IGNORECASE)

_REDACT_LOGGER_NAMES = (
    "httpx",
    "httpcore",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.connection",
    "httpcore.proxy",
)


class _BearerRedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001 — never break logging on format bugs
            return True
        redacted = _BEARER_RE.sub(r"\1***", msg)
        if redacted != msg:
            record.msg = redacted
            record.args = None
        return True


def _install_bearer_filter() -> None:
    for name in _REDACT_LOGGER_NAMES:
        logger = logging.getLogger(name)
        # Install once per logger — repeated ctor calls must not accumulate filters.
        if any(isinstance(f, _BearerRedactFilter) for f in logger.filters):
            continue
        logger.addFilter(_BearerRedactFilter())


_install_bearer_filter()


class AltegioClient:
    """Async-клиент к Altegio booking API. Единственный mutating endpoint — /book_record."""

    def __init__(
        self,
        config: AltegioConfig,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config
        self._external_http = http is not None
        self._http: httpx.AsyncClient | None = http
        self._closed = False

    @property
    def config(self) -> AltegioConfig:
        return self._config

    def __repr__(self) -> str:
        return (
            f"<AltegioClient base_url={self._config.base_url!r} "
            f"company_id={self._config.company_id} dry_run={self._config.dry_run}>"
        )

    async def __aenter__(self) -> AltegioClient:
        if self._closed:
            raise RuntimeError("AltegioClient already closed; create a new instance")
        if self._http is None:
            self._http = self._build_http_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._http is not None and not self._external_http:
            await self._http.aclose()
        self._closed = True

    def _build_http_client(self) -> httpx.AsyncClient:
        try:
            return httpx.AsyncClient(
                base_url=self._config.base_url,
                timeout=_DEFAULT_TIMEOUT,
                limits=_DEFAULT_LIMITS,
                http2=True,
            )
        except ImportError:
            _logger.warning(
                "h2 package not installed, HTTP/2 unavailable, falling back to HTTP/1.1"
            )
            return httpx.AsyncClient(
                base_url=self._config.base_url,
                timeout=_DEFAULT_TIMEOUT,
                limits=_DEFAULT_LIMITS,
                http2=False,
            )

    def _require_http(self) -> httpx.AsyncClient:
        if self._closed:
            raise RuntimeError("AltegioClient is closed")
        if self._http is None:
            raise RuntimeError(
                "AltegioClient not entered; use `async with AltegioClient(...) as c:`"
            )
        return self._http

    async def prearm(self) -> None:
        """Прогревает TLS-соединение к base_url. NoOp при dry_run=True.

        Идемпотентен. Игнорирует HTTP-статус (нам нужен только TCP+TLS handshake).
        """
        if self._config.dry_run:
            return
        http = self._require_http()
        try:
            await http.get("/", headers={"accept": "text/html"})
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            raise AltegioTransportError(f"prearm failed: {type(e).__name__}") from e
        except httpx.TransportError as e:
            raise AltegioTransportError(f"prearm failed: {type(e).__name__}") from e

    async def create_booking(
        self,
        *,
        service_id: int,
        staff_id: int,
        slot_dt_local: datetime,
        fullname: str,
        phone: str,
        email: str | None = None,
        timeout_s: float | None = None,
    ) -> BookingResponse:
        """Создаёт бронь. Hot path.

        Args:
            service_id: id услуги Altegio (> 0).
            staff_id: id корта/сотрудника (> 0).
            slot_dt_local: slot datetime в Asia/Almaty (tz-aware, tzinfo must be ALMATY).
            fullname: ФИО клиента, non-empty после strip.
            phone: телефон в формате 7XXXXXXXXXX, non-empty после strip.
            email: опционально; если None — ключ не отправляется.
            timeout_s: опциональный read-timeout override (для hot path).

        Raises:
            ValueError: pre-flight валидация (naive datetime, wrong TZ, empty name/phone, bad ids).
            AltegioBusinessError: сервер вернул 4xx (включая malformed 2xx-ответ).
            AltegioTransportError: 5xx, network error, non-JSON 2xx.
        """
        self._validate_inputs(
            service_id=service_id,
            staff_id=staff_id,
            slot_dt_local=slot_dt_local,
            fullname=fullname,
            phone=phone,
        )

        request = BookingRequest(
            fullname=fullname.strip(),
            phone=phone.strip(),
            email=email,
            bookform_id=self._config.bookform_id,
            appointments=[
                BookingAppointment(
                    services=[service_id],
                    staff_id=staff_id,
                    datetime=slot_dt_local.strftime("%Y-%m-%dT%H:%M:%S"),
                    available_staff_ids=[staff_id],
                )
            ],
        )

        if self._config.dry_run:
            _logger.info(
                "[DRY RUN] would POST %s body=%s",
                BOOK_RECORD_PATH.format(company_id=self._config.company_id),
                request.to_wire(),
            )
            return BookingResponse(record_id=0, record_hash="dry-run")

        return await self._post_booking(request, timeout_s=timeout_s)

    async def search_timeslots(
        self,
        *,
        date_local: date,
        staff_ids: list[int],
        timeout_s: float | None = None,
    ) -> list[TimeSlot]:
        """POST /api/v1/booking/search/timeslots/ — read-only probe.

        Возвращает массив слотов на `date_local` (Almaty local date) для кортов
        из `staff_ids`. Каждый слот содержит `is_bookable: bool`. Используется
        в poll-режиме для мониторинга освобождения слота.

        Идемпотентен. Dry-run — всё равно реальный POST (read-only, безопасно).

        Raises:
            ValueError: pre-flight (пустой/дублирующийся staff_ids, не-int).
            AltegioBusinessError: 4xx.
            AltegioTransportError: 5xx, network error, non-JSON 2xx.
        """
        self._validate_search_inputs(staff_ids)

        body: dict[str, Any] = {
            "context": {"location_id": self._config.company_id},
            "filter": {
                "date": date_local.strftime("%Y-%m-%d"),
                "records": [
                    {"staff_id": sid, "attendance_service_items": []}
                    for sid in staff_ids
                ],
            },
        }

        return await self._post_search_timeslots(body, timeout_s=timeout_s)

    @staticmethod
    def _validate_search_inputs(staff_ids: list[int]) -> None:
        if not staff_ids:
            raise ValueError("staff_ids must contain at least one id")
        for sid in staff_ids:
            if not isinstance(sid, int) or isinstance(sid, bool):
                raise ValueError(
                    f"staff_ids entries must be integers, got {type(sid).__name__}"
                )
            if sid < 1:
                raise ValueError(f"staff_ids entries must be >= 1, got {sid}")
        if len(set(staff_ids)) != len(staff_ids):
            raise ValueError(
                f"staff_ids must be unique, got duplicates in {list(staff_ids)}"
            )

    async def _post_search_timeslots(
        self, body: dict[str, Any], *, timeout_s: float | None
    ) -> list[TimeSlot]:
        http = self._require_http()
        headers = {
            "Authorization": f"Bearer {self._config.bearer_token.get_secret_value()}",
            "Content-Type": "application/json",
            "accept": "application/json, text/plain, */*",
        }
        kwargs: dict[str, Any] = {"json": body, "headers": headers}
        if timeout_s is not None:
            kwargs["timeout"] = httpx.Timeout(
                connect=_DEFAULT_TIMEOUT.connect,
                read=timeout_s,
                write=timeout_s,
                pool=_DEFAULT_TIMEOUT.pool,
            )

        try:
            response = await http.post(SEARCH_TIMESLOTS_PATH, **kwargs)
        except (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
            httpx.RemoteProtocolError,
            httpx.TransportError,
        ) as e:
            raise AltegioTransportError(type(e).__name__) from e
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — narrow to transport for unknown httpx issues
            raise AltegioTransportError(f"{type(e).__name__}: {e}") from e

        return self._parse_timeslots_response(response)

    @staticmethod
    def _parse_timeslots_response(response: httpx.Response) -> list[TimeSlot]:
        status = response.status_code

        if 500 <= status < 600:
            raise AltegioTransportError(f"server error {status}")

        content_type = response.headers.get("content-type", "")
        is_json = "application/json" in content_type.lower()

        if 400 <= status < 500:
            code, message = _extract_business_error(response, is_json=is_json)
            if status == 401 and code == "unknown":
                code = "unauthorized"
            raise AltegioBusinessError(code=code, message=message, http_status=status)

        if not (200 <= status < 300):
            raise AltegioTransportError(f"unexpected status {status}")

        if not is_json:
            raise AltegioTransportError(
                f"non-JSON 2xx response (content-type={content_type!r})"
            )
        try:
            body = response.json()
        except ValueError as e:
            raise AltegioTransportError(f"invalid JSON in 2xx: {e}") from e

        if isinstance(body, dict):
            data = body.get("data")
        elif isinstance(body, list):
            data = body
        else:
            raise AltegioBusinessError(
                code="malformed_success",
                message=f"unexpected top-level JSON type: {type(body).__name__}",
                http_status=status,
            )

        if data is None:
            return []
        if not isinstance(data, list):
            raise AltegioBusinessError(
                code="malformed_success",
                message=f"'data' must be a list, got {type(data).__name__}",
                http_status=status,
            )

        result: list[TimeSlot] = []
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}] must be a mapping, got {type(item).__name__}",
                    http_status=status,
                )
            attrs_raw = item.get("attributes")
            if not isinstance(attrs_raw, dict):
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}].attributes missing or not a mapping",
                    http_status=status,
                )

            dt_raw = attrs_raw.get("datetime")
            if not isinstance(dt_raw, str):
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}].attributes.datetime missing or not a string",
                    http_status=status,
                )
            try:
                parsed_dt = datetime.fromisoformat(dt_raw)
            except ValueError as e:
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}].attributes.datetime not ISO-8601: {dt_raw!r}",
                    http_status=status,
                ) from e
            if parsed_dt.tzinfo is None:
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}].attributes.datetime must be tz-aware: {dt_raw!r}",
                    http_status=status,
                )
            canonical_dt = parsed_dt.astimezone(ALMATY)

            is_bookable = attrs_raw.get("is_bookable")
            if not isinstance(is_bookable, bool):
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}].attributes.is_bookable must be bool",
                    http_status=status,
                )

            staff_id_raw = attrs_raw.get("staff_id")
            staff_id: int | None
            if staff_id_raw is None:
                staff_id = None
            elif isinstance(staff_id_raw, bool) or not isinstance(staff_id_raw, int):
                staff_id = None
            else:
                staff_id = staff_id_raw

            try:
                slot = TimeSlot(dt=canonical_dt, is_bookable=is_bookable, staff_id=staff_id)
            except ValidationError as e:
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"data[{idx}] invalid: {e.errors()}",
                    http_status=status,
                ) from e
            result.append(slot)
        return result

    @staticmethod
    def _validate_inputs(
        *,
        service_id: int,
        staff_id: int,
        slot_dt_local: datetime,
        fullname: str,
        phone: str,
    ) -> None:
        if slot_dt_local.tzinfo is None:
            raise ValueError("slot_dt_local must be timezone-aware")
        if slot_dt_local.tzinfo != ALMATY:
            raise ValueError(
                f"slot_dt_local must be in Asia/Almaty, got {slot_dt_local.tzinfo}"
            )
        if not fullname.strip():
            raise ValueError("fullname must not be empty")
        if not phone.strip():
            raise ValueError("phone must not be empty")
        if service_id <= 0:
            raise ValueError(f"service_id must be > 0, got {service_id}")
        if staff_id <= 0:
            raise ValueError(f"staff_id must be > 0, got {staff_id}")

    async def _post_booking(
        self, request: BookingRequest, *, timeout_s: float | None
    ) -> BookingResponse:
        http = self._require_http()
        path = BOOK_RECORD_PATH.format(company_id=self._config.company_id)
        headers = {
            "Authorization": f"Bearer {self._config.bearer_token.get_secret_value()}",
            "Content-Type": "application/json",
            "accept": "application/json, text/plain, */*",
        }
        kwargs: dict[str, Any] = {"json": request.to_wire(), "headers": headers}
        if timeout_s is not None:
            kwargs["timeout"] = httpx.Timeout(
                connect=_DEFAULT_TIMEOUT.connect,
                read=timeout_s,
                write=timeout_s,
                pool=_DEFAULT_TIMEOUT.pool,
            )

        try:
            response = await http.post(path, **kwargs)
        except (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
            httpx.RemoteProtocolError,
            httpx.TransportError,
        ) as e:
            raise AltegioTransportError(type(e).__name__) from e
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — narrow to transport for unknown httpx issues
            raise AltegioTransportError(f"{type(e).__name__}: {e}") from e

        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: httpx.Response) -> BookingResponse:
        status = response.status_code

        if 500 <= status < 600:
            raise AltegioTransportError(f"server error {status}")

        content_type = response.headers.get("content-type", "")
        is_json = "application/json" in content_type.lower()

        if 200 <= status < 300:
            if not is_json:
                raise AltegioTransportError(
                    f"non-JSON 2xx response (content-type={content_type!r})"
                )
            try:
                body = response.json()
            except ValueError as e:
                raise AltegioTransportError(f"invalid JSON in 2xx: {e}") from e

            if isinstance(body, list):
                if not body:
                    raise AltegioBusinessError(
                        code="malformed_success",
                        message="response array is empty",
                        http_status=status,
                    )
                first = body[0]
            elif isinstance(body, dict):
                first = body
            else:
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"unexpected top-level JSON type: {type(body).__name__}",
                    http_status=status,
                )

            try:
                return BookingResponse.model_validate(first)
            except ValidationError as e:
                raise AltegioBusinessError(
                    code="malformed_success",
                    message=f"response missing record_id/record_hash: {e.errors()}",
                    http_status=status,
                ) from e

        # 4xx path
        if 400 <= status < 500:
            code, message = _extract_business_error(response, is_json=is_json)
            if status == 401 and code == "unknown":
                code = "unauthorized"
            raise AltegioBusinessError(code=code, message=message, http_status=status)

        # 1xx / 3xx — unexpected, treat as transport.
        raise AltegioTransportError(f"unexpected status {status}")


_TEXT_CODE_MAPPING: tuple[tuple[str, str], ...] = (
    ("service is not available", "service_not_available"),
    ("unauthorized", "unauthorized"),
)


def _derive_code_from_text(text: str) -> str:
    """Подставляет business code из подстроки сообщения (case-insensitive).

    Altegio в новом shape (incident 24.04 02:00 UTC) возвращает только текст,
    без отдельного `code`. Маппинг — узкий: только зафиксированные строки.
    """
    lowered = text.lower()
    for needle, code in _TEXT_CODE_MAPPING:
        if needle in lowered:
            return code
    return "unknown"


def _extract_business_error(response: httpx.Response, *, is_json: bool) -> tuple[str, str]:
    """Парсит Altegio-style ошибку. Всегда возвращает (code, message).

    Altegio возвращает несколько shape (по убыванию приоритета):
      1. {"meta": {"errors": [{"code": "...", "message": "..."}]}} — старый array shape
      2. {"errors": {"code": <int|str>, "message": "..."}} — новый dict shape (24.04 incident)
      3. {"meta": {"message": "..."}, "success": false} — meta-only fallback
      4. Else → ("unknown", raw truncated body), WARN-лог.

    Для shapes 2 и 3 code derived через text mapping (см. `_derive_code_from_text`).
    """
    if not is_json:
        raw = response.text or "<empty>"
        _logger.warning(
            "altegio_unknown_error_body content_type=%r body=%r",
            response.headers.get("content-type"),
            _truncate(raw),
        )
        return "unknown", _truncate(raw)

    try:
        body: Any = response.json()
    except ValueError:
        raw = response.text or "<empty>"
        _logger.warning("altegio_unknown_error_body body=%r", _truncate(raw))
        return "unknown", _truncate(raw)

    if isinstance(body, dict):
        meta = body.get("meta")
        if isinstance(meta, dict):
            errors = meta.get("errors")
            if isinstance(errors, list) and errors:
                first = errors[0]
                if isinstance(first, dict):
                    code = str(first.get("code") or "unknown")
                    message = str(first.get("message") or "")
                    return code, message

        top_errors = body.get("errors")
        if isinstance(top_errors, dict):
            message_raw = top_errors.get("message")
            if isinstance(message_raw, str) and message_raw:
                return _derive_code_from_text(message_raw), message_raw

        if isinstance(meta, dict):
            top_message = meta.get("message")
            if isinstance(top_message, str) and top_message:
                return _derive_code_from_text(top_message), top_message

    raw = response.text or "<empty>"
    _logger.warning("altegio_unknown_error_body body=%r", _truncate(raw))
    return "unknown", _truncate(raw)


def _truncate(text: str, limit: int = _MAX_ERROR_BODY_CHARS) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"... (truncated, total={len(text)})"
