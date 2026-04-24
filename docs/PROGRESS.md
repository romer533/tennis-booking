# Прогресс реализации

> **Статус: MVP COMPLETE.** Весь код в `main`, готов к deploy. Полный план — [PLAN.md](PLAN.md). Deployment — [DEPLOYMENT.md](DEPLOYMENT.md).

## Замержено в main

| Фаза | PR | Что сделано |
|------|----|-------------|
| Phase 1 | [#1](https://github.com/romer533/tennis-booking/pull/1) | Скелет: pyproject (hatchling), ruff/mypy strict, GitHub Actions CI (Python 3.11+3.12), пакеты `src/tennis_booking/{altegio,scheduler,engine,profiles,config,obs}`, smoke-тест |
| Phase 3 — window | [#2](https://github.com/romer533/tennis-booking/pull/2) | `scheduler/window.py` — pure `next_open_window`. Правило T−3@07:00 Almaty. 110 тестов, 100% branch coverage |
| Phase 5 — config | [#3](https://github.com/romer533/tennis-booking/pull/3) | pydantic v2 schema (frozen, strict, extra=forbid), YAML loader, cross-validation, PII masking. 163 теста, 97% branch coverage |
| Phase 3 — Almaty rename | [#4](https://github.com/romer533/tennis-booking/pull/4) | `Atyrau → Almaty` rename, fix leap-year ожиданий под UTC+6 (Kazakhstan TZ unification 2024-03-01) |
| Phase 3 — clock | [#5](https://github.com/romer533/tennis-booking/pull/5) | `scheduler/clock.py` — async SNTP drift check (raw UDP). 33 unit + 1 integration, 100% branch coverage |
| Phase 2 — altegio | [#6](https://github.com/romer533/tennis-booking/pull/6) | `altegio/` — async httpx client, Bearer auth, `AltegioBusinessError`/`AltegioTransportError`, dry-run, `SecretStr` masking. 98 тестов, 98% branch coverage |
| Phase 4 — engine | [#7](https://github.com/romer533/tennis-booking/pull/7) | `engine/attempt.py` — `BookingAttempt` state machine. 2 CR раунда (race priority fix). 55 тестов, 97% coverage |
| Phase 3 — loop | [#8](https://github.com/romer533/tennis-booking/pull/8) | `scheduler/loop.py` — main daily loop, NTP guard, graceful shutdown, idempotency. service_id в config schema. 48 тестов, 95% coverage |
| Phase 7 — deployment | [#9](https://github.com/romer533/tennis-booking/pull/9) | `__main__.py`, RotatingFileHandler logs, systemd unit, sudoers, GitHub Actions CD, DEPLOYMENT.md. 19 новых тестов |

**Тестов в main:** 546 passed + 1 skipped + 1 deselected. Покрытие критичных модулей ≥ 95%.

## Готовность

- ✅ Код полностью реализован (Phase 1–5, 7).
- ✅ Тесты зелёные на CI (Python 3.11 + 3.12).
- ✅ Deployment artifacts готовы (`__main__.py`, systemd, GitHub Actions).
- ✅ Step-by-step deployment в [DEPLOYMENT.md](DEPLOYMENT.md).
- ⏳ **Нужны действия от пользователя**: SSH ключи + GitHub Secrets + одноразовая настройка сервера (~30 мин).
- ⏳ **NTP на сервере**: убедиться что drift < 50 мс (на dev-машине разработки был −2639 мс — `chronyc tracking` обязателен ДО старта).

## Phase 0 — Altegio API research

- [x] HAR проанализирован → `docs/api-research.md`
- [x] Hot path: единственный `POST /api/v1/book_record/521176`
- [x] SMS / captcha — отсутствуют
- [x] Город / TZ — Astana, `Asia/Almaty`
- [x] **Провокация #4**: достаточно `Authorization: Bearer <static>` — НЕ нужны antibot headers / реверс JS / headless Chromium
- [ ] Провокация #1: POST на слот через 4+ дней — закрытое окно (для tuning engine)
- [ ] Провокация #2: POST на занятый слот — точные коды (для `SLOT_TAKEN_CODES`)
- [ ] Провокация #3: дубль POST с интервалом 50 мс — идемпотентность Altegio
- [ ] Провокация #6: параллелизм 3-5 одновременных — rate-limit
- [ ] Провокация #7: момент перехода `is_bookable: false → true` — семантика 07:00

> Без оставшихся провокаций engine работает на fallback (unknown business code → `lost`). Тесты помечены TODO. Можно делать после первого успешного выигранного слота в проде.

## Phase 6 — Observability (отложено)

Что входит (когда понадобится — отдельный PR):
- Telegram-уведомления (won/lost/error per attempt + daily summary)
- SQLite history (для post-mortem и метрик success rate)
- Prometheus exporter (опционально)

Сейчас доступно: structlog JSON в `/var/log/tennis-booking/service.log` + `journalctl -u tennis-booking`.

## Tech debt — будущие cleanup PR

### Тесты
- [ ] `SntpClient._parse_response` — добавить unit-тесты с binary fixtures (PR #5)
- [ ] `test_concurrent_calls_independent` — переименовать или расширить на shared-state (PR #5)
- [ ] `test_yaml_error_without_mark` — настоящий path без mark остаётся непокрыт (PR #3)

### Код
- [ ] `_sntp.py:117-118` — fragile string-parsing при rewrap `NTPResponseError` (PR #5)
- [ ] `frozen_now` fixture — наследовать `_FrozenDatetime` от `datetime` для устойчивости (PR #5)
- [ ] `mask_phone` — docstring расходится с поведением для строк ≤ 8 символов (PR #3)
- [ ] Заполнить `NOT_OPEN_CODES` / `SLOT_TAKEN_CODES` в `engine/codes.py` после провокаций #1/#2

### Process / Infra
- [ ] Pre-commit hook (`ruff` + `mypy`) — отложен с Phase 1
- [ ] `config/profiles.example.yaml` — заменить `+77001234567` на безопасный плейсхолдер `+7-000-000-0000` (PR #1)
- [ ] CI integration job: отдельный `pytest -m integration` для NTP smoke

### Deployment (Phase 7 follow-up)
- [ ] `deploy/tennis-booking.sudoers` — заменить wildcard `*` для journalctl на строгий pattern (PR #9, low security risk)
- [ ] `setup-server.sh` `pip install` — добавить lockfile (`requirements.txt` с hashes) для воспроизводимых deploys (PR #9)
- [ ] NTP-синхронизация хоста — обязательное требование Phase 7 (drift > 50 мс → сервис не стартует)

## Что считается "готово к проду"

- [x] Код, тесты, CI зелёный
- [x] Логи с ротацией (10 MB × 14 = 140 MB cap)
- [x] systemd + auto-restart + hardening
- [x] Push → auto-deploy через GitHub Actions
- [ ] **SSH ключи + 5 GitHub Secrets** (от тебя)
- [ ] **Сервер настроен**: `python3.11`, `chrony`, `/opt/tennis-booking` clone, venv, systemd unit (от тебя через `setup-server.sh`)
- [ ] **`/etc/tennis-booking/env`** с реальным `ALTEGIO_BEARER_TOKEN` (от тебя)
- [ ] **`/etc/tennis-booking/schedule.yaml` + `profiles.yaml`** с реальным расписанием (от тебя)
- [ ] **NTP синхронизирован**: `chronyc tracking` offset < 50 мс (от тебя)
