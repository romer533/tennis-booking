# Прогресс реализации

> **Статус: В PROD.** Сервис работает 24/7 на `194.195.241.83`. Полный план — [PLAN.md](PLAN.md). Deployment — [DEPLOYMENT.md](DEPLOYMENT.md).

## Production hardening (post-MVP)

| PR | Что сделано |
|----|-------------|
| [#10–14](https://github.com/romer533/tennis-booking/pulls?q=is%3Apr+is%3Aclosed) | Court pools (fan-out по группе кортов), poll mode (мониторинг отмен за 3 дня), JSONL persistence (dedup против рестартов и manual bookings) |
| [#15](https://github.com/romer533/tennis-booking/pull/15) | Incident 25.04: parser N2 shape `errors={code,message}` + grace polling (15-мин retry после T−0) |
| [#16](https://github.com/romer533/tennis-booking/pull/16) | Cross-profile dedup fix: same slot OK для разных profiles (key включает profile_name) |
| [#17](https://github.com/romer533/tennis-booking/pull/17) | min_lead_time guard: skip fire если slot < N часов (default 2) — free-cancel window |
| [#18](https://github.com/romer533/tennis-booking/pull/18) | LEAD_DAYS 3 → 2: empirically confirmed Altegio horizon = today + 2 calendar days (через `search_dates` API) |
| [#19](https://github.com/romer533/tennis-booking/pull/19) | **Incident 26.04**: parser fall-through на incomplete legacy stub `meta.errors=[{}]` + relax grace на ANY service_not_available (вместо ALL) |
| [#20](https://github.com/romer533/tennis-booking/pull/20) | **Incident 27.04**: parser text mapping — добавлен `"no staff members available for booking"` → `service_not_available`. Altegio начал слать новый текст ошибки, который попадал в `unknown` → fallback `lost`. Одна строка в `_TEXT_CODE_MAPPING` + 5 regression тестов |
| [#21](https://github.com/romer533/tennis-booking/pull/21) | **Cloudflare detector**: 6% запросов 28.04 fire получали `403 + text/html + Just a moment...` (Altegio за Cloudflare). Раньше → `unknown` → fallback `lost`. Теперь → `AltegioTransportError(cause="cloudflare_challenge")` → engine retry до global_deadline. 13 тестов |
| [#22](https://github.com/romer533/tennis-booking/pull/22) | **Post-window poll**: после проигранного window phase scheduler сразу планировал следующую неделю, забывая текущий слот. Теперь — продолжает поллить `search_timeslots` каждые 120s до `slot - min_lead_time_hours`, ловя отмены. `PollAttempt` параметризован `post_window_mode`. Restart resilience через `_maybe_restart_post_window_poll`. Kill switch `TENNIS_POST_WINDOW_POLL_ENABLED` (default true). 24 теста |
| [#23](https://github.com/romer533/tennis-booking/pull/23) | **Shared poll cache + jitter**: 21 polls делали `search_timeslots` одновременно каждые 120s — Cloudflare overload risk. `PollResultCache` с key `(date, pool)` дедуплицирует concurrent fetches через per-key `asyncio.Lock`. Initial jitter `U(0, interval/2)` + per-tick ±10% распределяет burst. Verified в проде: **10x reduction** (3 HTTP fetches per cycle вместо 21), **91.4% hit ratio**. 27 тестов |
| [#24](https://github.com/romer533/tennis-booking/pull/24) | **cf-ray logging**: `_log_cloudflare_challenge` теперь пишет `cf_ray`, `cf_mitigated`, `cf_cache_status` headers. Подготовка к whitelist request админу Daulet — нужны конкретные cf-ray IDs для CF support trace. 4 теста |
| [#25](https://github.com/romer533/tennis-booking/pull/25) | **Defensive Cloudflare mitigation**: Daulet отказал в whitelist → переключаемся на снижение burst rate. (1) `BookingRule.max_parallel_shots: int\|None` — cap fan-out до random subset (config: 3). Initial burst 77 → 33 POSTs. (2) Exponential backoff на transport retry: CF cause 100/200/400/800/1600/2000ms cap 2000, other transport 50/100/200/400/500ms cap 500. Per-shot tasks — backoff не блокирует sibling. Skip retry если deadline-now < delay+0.1s. 25 тестов. Schedule.yaml на сервере обновлён `max_parallel_shots: 3` для всех 59 bookings. **Verified в проде (30.04 fire):** CF block rate 75% → 0.2% (375× reduction), 1 win через window phase впервые! |
| [#26](https://github.com/romer533/tennis-booking/pull/26) | **Poll cap parallel_shots** (PR #25 follow-up): cap не применялся в `PollAttempt._fire_shots`. Production observation 30.04 11:23: post-window poll detected bookable → 7 shots без cap → 1 win + 1 duplicate (646335329). PR симметрично применяет cap в poll fire path. Все 4 phase теперь под cap (window, post-window, pre-window poll, post-window poll). 6 тестов |
| [#27](https://github.com/romer533/tennis-booking/pull/27) | **Auto-cancel duplicate bookings**: user reverses предыдущее решение "отменю руками" — после 2 duplicates просит автоматическую отмену. `AltegioClient.cancel_booking(record_id, record_hash)` — DELETE на predicted URL `/api/v1/booking/locations/{company_id}/attendances/{record_id}/?hash=...&bookform_id=...`. `_cancel_duplicates` helper в window/grace/poll phase — best-effort с verbose logging (`cancel_response_status_code` field). Feature flag `TENNIS_CANCEL_DUPLICATES_ENABLED` default `true`. URL — predicted, требует verification после первого prod-cancel. 27 тестов |

## Замержено в main (MVP)

| Фаза | PR | Что сделано |
|------|----|-------------|
| Phase 1 | [#1](https://github.com/romer533/tennis-booking/pull/1) | Скелет: pyproject (hatchling), ruff/mypy strict, GitHub Actions CI (Python 3.11+3.12), пакеты `src/tennis_booking/{altegio,scheduler,engine,profiles,config,obs}`, smoke-тест |
| Phase 3 — window | [#2](https://github.com/romer533/tennis-booking/pull/2) | `scheduler/window.py` — pure `next_open_window`. Правило T−2@07:00 Almaty (исправлено в PR #18). 110 тестов, 100% branch coverage |
| Phase 5 — config | [#3](https://github.com/romer533/tennis-booking/pull/3) | pydantic v2 schema (frozen, strict, extra=forbid), YAML loader, cross-validation, PII masking. 163 теста, 97% branch coverage |
| Phase 3 — Almaty rename | [#4](https://github.com/romer533/tennis-booking/pull/4) | `Atyrau → Almaty` rename, fix leap-year ожиданий под UTC+6 (Kazakhstan TZ unification 2024-03-01) |
| Phase 3 — clock | [#5](https://github.com/romer533/tennis-booking/pull/5) | `scheduler/clock.py` — async SNTP drift check (raw UDP). 33 unit + 1 integration, 100% branch coverage |
| Phase 2 — altegio | [#6](https://github.com/romer533/tennis-booking/pull/6) | `altegio/` — async httpx client, Bearer auth, `AltegioBusinessError`/`AltegioTransportError`, dry-run, `SecretStr` masking. 98 тестов, 98% branch coverage |
| Phase 4 — engine | [#7](https://github.com/romer533/tennis-booking/pull/7) | `engine/attempt.py` — `BookingAttempt` state machine. 2 CR раунда (race priority fix). 55 тестов, 97% coverage |
| Phase 3 — loop | [#8](https://github.com/romer533/tennis-booking/pull/8) | `scheduler/loop.py` — main daily loop, NTP guard, graceful shutdown, idempotency. service_id в config schema. 48 тестов, 95% coverage |
| Phase 7 — deployment | [#9](https://github.com/romer533/tennis-booking/pull/9) | `__main__.py`, RotatingFileHandler logs, systemd unit, sudoers, GitHub Actions CD, DEPLOYMENT.md. 19 новых тестов |

**Тестов в main:** 1118 passed + 1 skipped + 1 deselected. Покрытие критичных модулей ≥ 95%.

## Production status

- ✅ Сервис работает на `194.195.241.83:13022` (Docker + systemd, auto-restart)
- ✅ GitHub Actions CD: push в main → build → push в ghcr.io → ssh restart
- ✅ NTP синхронизирован (`chronyc tracking` offset < 50 мс)
- ✅ Текущее расписание: 59 bookings (3 профиля: roman/askar/alena, корты indoor + outdoor)
- ✅ Persistence: `bookings.jsonl` для dedup (против рестартов и manual bookings)
- ✅ Manual booking подтверждён: record_id 645327563 (26.04 23:00 outdoor)
- ✅ **Первая автоматическая бронь (27.04 02:00 UTC):** record_id 645621093 (roman, ср 29.04 07:00 indoor). 1 win из 11 attempts — остальные lost из-за N4 incident (см. ниже).
- ✅ **Победа poll mode (28.04 01:59:55 UTC):** record_id 645847642 (roman, чт 30.04 07:00 indoor) — за 5 секунд до открытия окна. Бонус-дубль 645847641 на другом корте → отменён вручную (user предпочитает manual cancel вместо auto-cancel). 0/10 wins в window mode из-за Cloudflare 403 (см. PR #21).
- ✅ **Третья победа (29.04 02:06:38 UTC):** record_id 646042815 (roman, пт 01.05 07:00 indoor) — выиграл уже после window phase, через ~6 минут после открытия. 1 win из 11 attempts — Cloudflare заблокировал 75% запросов (1680 hits на /book_record/, 0 на /search/timeslots/). См. N5 incident.
- ✅ **Четвёртая победа (30.04 02:00:00.867 UTC):** record_id 646234414 (roman, сб 02.05 12:00 indoor) — **первый раз через WINDOW phase** за <1 секунды после открытия. PR #25 defensive снизил CF block с 75% до 0.2%.
- ✅ **Пятая и шестая победы (30.04 11:21-11:23 UTC):** record_id 646334804 + 646335328 (askar, сб 02.05 21:00 + 20:00 indoor) — пойманы post-window poll'ом (PR #22) после real cancellations других людей. +1 duplicate 646335329 (court 1521555) — gap в PR #25 cap для poll path → закрыт PR #26.

## Production incidents

| Дата | Suspect | Root cause | Fix |
|------|---------|------------|-----|
| 25.04 02:00 UTC | All shots `unknown_code` → lost | Parser не знал shape `errors={"code":N,"message":"..."}` | PR #15 (parser N2 shape + grace polling) |
| 26.04 02:00 UTC | Mix snv + unknown → lost (grace blocked) | (1) Parser early-return на incomplete legacy stub `meta.errors=[{}]`; (2) grace требовал ALL snv | PR #19 (parser fall-through + grace ANY snv) |
| 27.04 02:00 UTC | 10/11 attempts lost code=`unknown` | Altegio начал слать новый текст `"Currently, there are no staff members available for booking"` — `_derive_code_from_text` его не маппил | PR #20 (одна строка в `_TEXT_CODE_MAPPING`). Lesson: всегда смотреть raw body перед изобретением сложных fix'ов — чуть не ушёл в N3 v2 retry-релакс. |
| 28.04 02:00 UTC | 0/10 window wins, 1 poll win + dup | (1) Altegio за Cloudflare antibot — 6% запросов получали 403+html challenge → engine fallback `lost code=unknown`. (2) Engine не отменяет дубликаты при multi-success fan-out (user предпочёл manual cancel). | PR #21 (Cloudflare detector → transport retry). Cancel-fix отложен. |
| 29.04 02:00 UTC | 1 win (poll), 10 timeout (Cloudflare retry storm) | Cloudflare ввёл агрессивный rate-limit на `/book_record/` — 75% запросов (1680/2220) получили 403 за 8-секундное окно. PR #21 retry'ил каждый, raising sustained 6 RPS на одном IP. Research доказал: НЕ TLS fingerprint (curl_cffi=httpx 0/50 блоков), НЕ persistent IP rep (search/timeslots ОК) — а **rate-rule на write endpoint**. | PR #24 (cf-ray logging для CF support trace). Primary fix: whitelist IP `194.195.241.83` через Daulet admin. Phase 1.5 (Playwright) выкинут — не помогает rate-rule. |

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
