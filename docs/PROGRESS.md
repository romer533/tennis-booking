# Прогресс реализации

> Живой статус. Полный план — [PLAN.md](PLAN.md). Обновляется по мере PR.

## Замержено в main

| Фаза | PR | Что сделано |
|------|----|-------------|
| Phase 1 | [#1](https://github.com/romer533/tennis-booking/pull/1) | Скелет проекта: pyproject (hatchling), ruff/mypy strict, GitHub Actions CI (Python 3.11+3.12), пустые пакеты `src/tennis_booking/{altegio,scheduler,engine,profiles,config,obs}`, smoke-тест, example YAML |
| Phase 3 — window | [#2](https://github.com/romer533/tennis-booking/pull/2) | `scheduler/window.py` — pure `next_open_window(slot_local_dt) -> datetime`. Правило T−3@07:00 Almaty. 110 тестов + 1 skipped, 100% branch coverage |
| Phase 5 — config | [#3](https://github.com/romer533/tennis-booking/pull/3) | pydantic v2 schema (frozen, strict, extra=forbid), YAML loader, cross-validation, PII masking. 163 теста, 97% branch coverage |
| Phase 3 — Almaty rename | [#4](https://github.com/romer533/tennis-booking/pull/4) | `Atyrau → Almaty` rename, fix leap-year ожиданий под UTC+6 (Kazakhstan TZ unification 2024-03-01) |
| Phase 3 — clock | [#5](https://github.com/romer533/tennis-booking/pull/5) | `scheduler/clock.py` — async SNTP drift check (raw UDP), `ClockDriftError`, `NTPUnreachableError`, `NTPResponseError`. 33 unit + 1 integration, 100% branch coverage |
| Phase 2 — altegio | [#6](https://github.com/romer533/tennis-booking/pull/6) | `altegio/` — async httpx client, Bearer auth, `AltegioBusinessError`/`AltegioTransportError`, dry-run, `SecretStr` masking, `_BearerRedactFilter` на httpx+httpcore.*. 98 тестов, 98% branch coverage |

**Тестов в main:** 403 passed + 1 skipped + 1 deselected.

## В работе (CR / CI)

| Фаза | Ветка / PR | Статус |
|------|------------|--------|
| Phase 4 — `engine/` | [#7](https://github.com/romer533/tennis-booking/pull/7) | CI pending (prior push-run зелёный). `BookingAttempt` state machine, 2 CR рaunds (race priority fix), 55 новых тестов, 97% coverage |

## Заблокировано

| Что | Кем блокировано | Кто разблокирует |
|-----|-----------------|------------------|
| Phase 3 — `scheduler/loop.py` | Phase 4 merge (нужен `BookingAttempt.run()`) | — |
| Phase 6 — observability | Phase 4 | — |
| Phase 7 — deploy | Phase 6 | — |

## Phase 0 — Altegio API research (в работе)

- [x] HAR проанализирован → `docs/api-research.md` v1
- [x] Hot path выявлен: единственный `POST /api/v1/book_record/521176`
- [x] SMS / captcha — отсутствуют (Phase 1.5 отпадает)
- [x] Город / TZ — Astana, `Asia/Almaty` (исправлено в коде PR #4)
- [x] **Провокация #4**: POST без `x-app-signature` / `x-app-client-context` — **DONE**. Достаточно `Authorization: Bearer <static>`. Реверс JS / headless Chromium НЕ нужны.
- [ ] Провокация #1: POST на слот через 4+ дней (закрытое окно — реакция API)
- [ ] Провокация #2: POST на занятый слот (response shape)
- [ ] Провокация #3: дубль POST с интервалом 50 мс (идемпотентность Altegio)
- [ ] Провокация #6: параллелизм 3-5 одновременных POST на тестовый слот (rate-limit)
- [ ] Провокация #7: момент перехода `is_bookable: false → true` (точная семантика 07:00)

## Tech debt — будущие cleanup PR

Накопленные nit / non-blocking замечания CR. Группируются в follow-up PR:

### Тесты
- [ ] `SntpClient._parse_response` — добавить unit-тесты с binary fixtures (PR #5)
- [ ] `test_concurrent_calls_independent` — переименовать в `test_parallel_calls_with_separate_clients_ok` или расширить на shared-state checks (PR #5)
- [ ] `test_yaml_error_without_mark` — фактически тестирует `MarkedYAMLError` path; настоящий path остаётся непокрыт (PR #3)

### Код
- [ ] `_sntp.py:117-118` — fragile string-parsing при rewrap `NTPResponseError`. Передавать `server` в `_parse_response` напрямую (PR #5)
- [ ] `frozen_now` fixture в `tests/conftest.py` — наследовать `_FrozenDatetime` от `datetime`, чтобы расширение `clock.py` другими методами `datetime` не сломало тесты (PR #5)
- [ ] `mask_phone` — docstring обещает "первые 4 + *** + последние 4", но для строк ≤ 8 символов возвращает только `***` (PR #3)

### Process / Infra
- [ ] Pre-commit hook (`ruff` + `mypy`) — отложен с Phase 1 как отдельный PR
- [ ] `config/profiles.example.yaml` — заменить `+77001234567` на гарантированно-нерезервируемый плейсхолдер `+7-000-000-0000` (PR #1)
- [ ] CI integration job (опционально): отдельный shaft `pytest -m integration` с continue-on-error для NTP smoke

### Deploy (Phase 7)
- [ ] NTP-синхронизация хоста — на dev-машине обнаружен реальный drift −2639 мс (PR #5 integration test). Без NTPd на проде сервис будет регулярно ловить `ClockDriftError`
