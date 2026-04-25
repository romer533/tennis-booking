"""Классификация бизнес-кодов Altegio для engine retry-logic.

Коды derived парсером altegio/client.py:_extract_business_error из shape:
- {"errors": {"code": ..., "message": "..."}} → text mapping
- {"meta": {"errors": [...]}}                 → first.code (legacy)
- {"meta": {"message": "..."}}                → text mapping

Никакой нормализации строк в engine нет: сравнение строго по значению.
"""
from __future__ import annotations

__all__ = ["CONFIG_ERROR_CODES", "NOT_OPEN_CODES", "SLOT_TAKEN_CODES"]

# "Слот ещё не открыт на бронирование" → engine ретраит каждые not_open_retry_ms
# до not_open_deadline_s после T−0; затем (если задан grace_polling) переходит
# в grace mode на period_s.
NOT_OPEN_CODES: frozenset[str] = frozenset({"service_not_available"})

# "Слот уже занят другим клиентом" → engine фиксирует loss мгновенно, без ретраев.
# TODO: Altegio в обнаруженных shapes возвращает тот же текст, что и для not_open
# ("service is not available"). Разделить, если найдётся отдельный код.
SLOT_TAKEN_CODES: frozenset[str] = frozenset()

# Конфигурационные/авторизационные ошибки — engine не ретраит, status=error.
# "unauthorized" мапится клиентом из HTTP 401 (см. altegio/client.py _parse_response).
CONFIG_ERROR_CODES: frozenset[str] = frozenset({"unauthorized"})
