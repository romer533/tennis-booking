"""Классификация бизнес-кодов Altegio для engine retry-logic.

Точные строки кодов ещё не зафиксированы — провокации #1 (slot ещё не открыт)
и #2 (slot уже занят) в docs/api-research.md не проведены. Пока frozenset'ы
пустые → engine обрабатывает такие неизвестные коды как lost (fallback).

После провокаций — добавить сюда exact-match строки из поля `code` ответа сервера.
Никакой нормализации строк в engine нет: сравнение строго по значению.
"""
from __future__ import annotations

__all__ = ["CONFIG_ERROR_CODES", "NOT_OPEN_CODES", "SLOT_TAKEN_CODES"]

# "Слот ещё не открыт на бронирование" → engine ретраит каждые not_open_retry_ms
# до not_open_deadline_s после T−0. Заполняется после провокации #1.
NOT_OPEN_CODES: frozenset[str] = frozenset()

# "Слот уже занят другим клиентом" → engine фиксирует loss мгновенно, без ретраев.
# Заполняется после провокации #2.
SLOT_TAKEN_CODES: frozenset[str] = frozenset()

# Конфигурационные/авторизационные ошибки — engine не ретраит, status=error.
# "unauthorized" мапится клиентом из HTTP 401 (см. altegio/client.py _parse_response).
CONFIG_ERROR_CODES: frozenset[str] = frozenset({"unauthorized"})
