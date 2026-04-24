# Исследование Altegio API — booking flow

> **Статус:** Phase 0, draft v1. Заполнено по HAR пользователя `b551098.alteg.io.har` от 2026-04-24, реальная бронь, успешный happy path. Шаблон: [research-template.md](research-template.md).
>
> **Ключевые открытые вопросы — в конце документа.** Без провокаций (закрытый слот, занятый, дубликат, без headers, параллелизм) Phase 0 не закрывается. См. чек-лист "Гейт перед закрытием".

## Окружение исследования

- Дата проведения: **2026-04-24** (пятница), 13:00:45 UTC = 18:00:45 Asia/Almaty (UTC+5).
- Браузер: Chrome 147.0.0.0 / Windows 11.
- HAR-источник: `C:\Users\romer\Downloads\b551098.alteg.io.har` (~25 MB, 158 XHR/fetch entries).
- Аккаунт: фактически анонимный — за весь флоу **ни одной cookie не было ни послано, ни выставлено сервером** (см. раздел "Сводка → Авторизация").
- Тестовый слот, реально забронированный пользователем:
  - Корт: **Корт №5**, `staff_id = 1521566`, position `Открытый` (`position_id = 180845`).
  - Услуга: **«Аренда открытого корта (1 час)»**, `service_id = 7849893`, цена 2500 KZT.
  - Дата/время: **2026-04-26 23:00 Asia/Almaty** (воскресенье, 23:00; в формате wire — `2026-04-26T23:00:00+05:00`).
  - Получено `record_id = 645268016`, `record_hash = 554f7a6a693197209816116ea42f3b09`, человеческий токен `RPDQO`.
- Клуб (важно — корректно зафиксировать):
  - `company_id = 521176`, `bookform_id = 551098`, `group_id = 0`, `main_group_id = 503482` («Сеть Daulet Tennis Academy»).
  - **Город: Астана** (`city_id = 73`, `country_id = 8` — Казахстан). НЕ Atyrau. CLAUDE.md, где упоминается Atyrau, нужно поправить отдельно.
  - Юрлицо: ГККП «Даулет» акимата города Нур-Султан, ул. Кордай, 6.
  - Timezone: `Asia/Almaty`, fixed offset UTC+5 (без DST). В ответе `/company/521176` поле `timezone: 5`, в `/book_record` ответе — `timezone: 6` (sic; используется по-разному в разных эндпоинтах, но реальные `datetime` всегда возвращаются с суффиксом `+05:00` или `+0500`, и только так).

## Окно открытия слотов (важная находка)

В ответе `/api/v1/booking/search/dates` (запрос #28, без фильтра по `staff_id`) при `date_from=2026-04-24` сервер вернул `is_bookable: true` только для **2026-04-24, 2026-04-25, 2026-04-26**. Все даты с 27.04 и далее — `is_bookable: false`. То есть в момент исследования (24.04 в 18:00 Astana) горизонт открытия = **«сегодня + 2 дня вперёд»**, итого ровно **3 даты**.

Это означает:
- Слот воскресенья 26.04 23:00 был открыт уже как минимум с 24.04 (а скорее всего — с 23.04 утра, когда впервые стало видно «сегодня + 3 дня вперёд» = 26.04). Точно момент открытия из HAR определить нельзя.
- Под предположение "окно открывается за 3 дня в 07:00 локального времени" HAR не противоречит, но и не подтверждает. **Нужна провокация**: запросить `/api/v1/booking/search/dates` с `date_from = T` для даты T = сейчас+3 дня в течение нескольких часов вокруг 07:00 Astana и зафиксировать момент перехода `is_bookable: false → true`.
- HTML-инжекшн в `/api/v1/bookform/551098/` явно указывает на ограничение клуба: "Мы принимаем бронирования на ближайшие три дня. К сожалению, на текущую дату бронирование недоступно".

## Хронология запросов booking-флоу

Из 158 XHR/fetch:
- **35** — реальные API-запросы к `b551098.alteg.io` (предмет интереса).
- ~50 — телеметрия `tracks.alteg.io/api/v1/track` (счётчики UX-кликов).
- ~20 — Sentry `errors.alteg.io/api/38/envelope/` (фронтовые ошибки).
- ~14 — Google Analytics `www.google-analytics.com/g/collect`.
- Остальное — статика (svg-иконки).

Ниже задокументированы только запросы к API клуба (`b551098.alteg.io`). Группы: (A) bootstrap страницы, (B) выбор корта/услуги/времени, (C) подготовка контактного шага, (D) **создание брони** — главный запрос, (E) пост-брони полло.

---

## Группа A. Bootstrap при открытии страницы

При первом открытии `https://b551098.alteg.io/company/521176/personal/menu` фронт параллельно дёргает 8 GET-эндпоинтов. Это «прогревочный» набор — он создаёт UX, но НИ ОДИН из них не выставляет cookie или сессию. Их можно полностью пропустить, если знаем все нужные ID заранее.

### Запрос 1: GET /api/v1/bookform/551098/

**Когда происходит:** первый запрос при открытии страницы.

**Зачем нужен:** конфигурация виджета — порядок шагов, тексты, флаги фич (`sms_enabled`, `phone_confirmation`, `is_show_privacy_policy`, `is_client_agreements_feature_enabled`, `ab_test_enabled`).

**Метод и URL:**
```
GET https://b551098.alteg.io/api/v1/bookform/551098/
```

**Query-параметры:** нет.

**Request headers (значимые):**
```
accept: application/json, text/plain, */*
accept-language: ru-RU
referer: https://b551098.alteg.io/company/521176/personal/menu?o=
sec-fetch-site: same-origin
user-agent: Mozilla/5.0 (Windows NT 10.0; ...) Chrome/147.0.0.0 Safari/537.36
x-altegio-application-name: client.booking
x-altegio-application-platform: angular-18.2.13
x-altegio-application-version: 199620.2b0fce8b
x-altegio-application-action:           (пусто)
x-app-signature:                        (пусто)
x-app-validation-token:                 (пусто)
baggage: sentry-environment=live,sentry-release=199620.2b0fce8b,...   (Sentry-инструментация, опциональна)
sentry-trace: <trace_id>-<span_id>-0                                  (Sentry, опциональна)
```
Статичные: `x-altegio-application-name`, `-platform`, `-version`, `accept`, `user-agent`. Динамические: `sentry-trace`, `baggage` (генерятся фронтом для Sentry — сервер на них не смотрит).

**Request body:** нет (GET).

**Response (success):**
- статус: `200`
- response headers: `content-type: application/json`, `cache-control: public, max-age=2`, `x-request-id: <uuid>`, `strict-transport-security`. **`Set-Cookie`: отсутствует.**
- ключевые поля ответа (важные для дальнейших шагов):
  - `id: 551098` (= bookform_id, понадобится в `book_record` payload).
  - `company_id: 521176` (= location_id, базовый параметр всех запросов).
  - `phone_confirmation: false` — **ключ: SMS-код для подтверждения телефона НЕ требуется**.
  - `sms_enabled: true` — но это про SMS-уведомления, не про подтверждение.
  - `is_client_agreements_feature_enabled: false` — никаких чекбоксов «согласия» обязательно подписывать не надо.
  - `is_online_sale_available: false` — оплата онлайн отключена → `prepaid: forbidden` для всех услуг.
  - `is_show_privacy_policy: true` — показ ссылки на privacy policy, но не блокирующий чекбокс.
  - `steps`: порядок UX-шагов (`master → service → datetime → contact → confirm`); значимо для фронта, не для backend.

**Response (errors):** не воспроизводилось.

**Заметки:**
- Кешируется на стороне CDN (max-age=2). Можно дёргать один раз и переиспользовать.
- `injection.content` содержит сырой `<script>...</script>` с JS, который клуб подмешивает (замена слов "специалист" → "корт" и т.д.). Для нас бесполезно.
- НЕ требуется для `book_record` POST как таковой — но даёт `phone_confirmation` и `is_client_agreements_feature_enabled`, по которым мы понимаем, можно ли стрелять hot path без SMS.

---

### Запрос 2: GET /api/v1/booking/forms/551098/custom_fields

**Когда:** одновременно с #1.
**Зачем:** список кастомных полей, которые форма может потребовать заполнить. **Ответ пустой** (`{"data":[]}`) → клуб не настроил кастомные поля → можно отправлять `custom_fields: {}` в `book_record`.

**Метод/URL:** `GET /api/v1/booking/forms/551098/custom_fields`. Headers те же (без cookies, без auth, без signature). 200 OK.

---

### Запрос 3: GET /api/v1/booking/forms/551098/security_levels/

**Когда:** одновременно с #1.
**Зачем:** настройки требуемого уровня верификации клиента (e.g., обязателен ли SMS, captcha). **Ответ:** `{"success":true,"data":[],"meta":{"count":0}}` — пусто. Подтверждает: клуб security-фичи не настраивал.

**Заметки:** факт того, что список пуст, очень важен — это аргумент в пользу того, что booking POST реально может пройти без SMS / captcha.

---

### Запрос 4: GET /api/v1/i18n/ru-RU

**Когда:** при загрузке. **Зачем:** локализация фронта. **Размер:** 45 KB JSON. Для backend бесполезно, **можно полностью игнорировать в нашем клиенте**.

---

### Запрос 5: GET /api/v1/booking/locations/521176/applications/available

Список ссылок на мобильные приложения (`Aunio` — собственное приложение Altegio). Не используется в booking. Игнорируем.

---

### Запрос 7: GET /api/v1/company/521176?forBooking=1&bookform_id=551098&include=has_cashback

**Зачем:** мета-данные клуба (название, адрес, телефоны, расписание, активный staff_count, флаги).

**Ключевые поля ответа:**
- `title: "Daulet Tennis Academy"`, `public_title: "Теннисный Центр Даулет"`, `country_id: 8`, `city_id: 73`, `city: "Астана"`, `address: "ул. Кордай, 6"`, `timezone: 5`, `timezone_name: "Asia/Almaty"`, `phone: "+7 706 641-04-85"`, `currency_short_title: "₸"`.
- `phone_confirmation: false`, `sms_enabled: true`, `is_offline_record_notification_enabled: false`.
- `record_type_id: 0`.
- `next_slot: "2026-04-24T18:05:00+0500"` — ближайший доступный слот (на момент 18:00:46 Astana).
- `active_staff_count: 16` (16 кортов).

**Не нужен** для собственно создания брони, но даёт sanity-check (timezone, валюта, активность клуба).

---

### Запрос 8: GET /api/v1/company/521176/promo_blocks → `{"success":true,"data":[]}`. Игнорируем.

### Запрос 9: GET /api/v1/booking/locations/521176/privacy_policy

40 KB HTML текста "Пользовательское соглашение Клуба «Теннисный центр Даулет»". `is_privacy_agreement_enabled: false` — соглашаться программно не нужно.

### Запрос 10: GET /api/v1/book_services/521176?without_seances=1

**Каталог услуг.** Возвращает 3 услуги:
| service_id | title | price | seance_length | category_id |
|-----------|-------|-------|---------------|-------------|
| 7790744 | Аренда крытого корта (1 час) | 3500 | null (=1ч) | 7790743 |
| 7849893 | **Аренда открытого корта (1 час)** | 2500 | null (=1ч) | 7790743 |
| 7854914 | Аренда стенки | 1000 | null (=1ч) | 7790743 |

Все: `prepaid: "forbidden"`, `abonement_restriction: 0`, `is_composite: false`, одна категория `7790743 "Аренда теннисного корта"`. Для нашей цели — **запоминаем `7849893` (открытый корт)**, остальные не нужны.

### Запрос 11: GET /api/v1/book_staff/521176?datetime=&without_seances=1

**Каталог кортов** (16 «сотрудников» = 16 кортов). У каждого:
- `id`, `name` (например "Корт 5"), `specialization` ("Крытый"/"Открытый"), `position.id` (180844 = крытый, 180845 = открытый), `bookable: true|false`, `schedule_till: "2026-05-31"`, `prepaid: "forbidden"`.
- Open courts (по spec из ответа): id 1521562, 1521564, 1521565, 1521566, 1521567 (5 уличных).
- Indoor courts: 1513587 ("Корт 1"), 1521552, 1521553, 1521555, 1521557, 1521558, 1521559, 1521561, 1529269..1529271 (11 крытых).

**Для нас:** список ID открытых кортов нужно жёстко знать (или фильтровать по `position.title == "Открытый"` после периодического обновления каталога).

---

## Группа B. Выбор слота (search/*)

После загрузки страницы фронт начинает переключаться между шагами и каждый раз дёргает связку `search/services` + `search/staff` + `search/dates` + `search/timeslots` с инкрементальным сужением фильтров. Это та же подсистема `/api/v1/booking/search/*`. Все 4 эндпоинта POST с JSON-телом одной и той же формы:

```jsonc
{
  "context": {"location_id": 521176},
  "filter": {
    "datetime": "2026-04-26T23:00:00+05:00",   // или null
    "date":      "2026-04-26",                  // только в /timeslots
    "date_from": "2026-04-24",                  // только в /dates
    "date_to":   "9999-01-01",                  // только в /dates
    "records": [
      {
        "staff_id": 1521566,                    // или null
        "attendance_service_items": [
          {"type": "service", "id": 7849893}    // или [] если не выбрано
        ]
      }
    ]
  }
}
```

Headers идентичны bootstrap-запросам (без cookies, без signature, добавлен `content-type: application/json` и `origin: https://b551098.alteg.io`).

### Запрос B1: POST /api/v1/booking/search/services/

**Зачем:** список service_id, доступных под текущий фильтр (любой staff / выбранный staff / выбранный datetime).
**Body пример** (#24, без выбранного staff/datetime):
```json
{"context":{"location_id":521176},"filter":{"records":[{"attendance_service_items":[]}]}}
```
**Response пример:**
```json
{"data":[
  {"type":"booking_search_result_services","id":"7790744","attributes":{"is_bookable":true,"bookable_status":"bookable","duration":3600,"price_min":3500,"price_max":3500}},
  {"type":"booking_search_result_services","id":"7849893","attributes":{"is_bookable":true,"bookable_status":"bookable","duration":3600,"price_min":2500,"price_max":2500}},
  {"type":"booking_search_result_services","id":"7854914","attributes":{"is_bookable":true,"bookable_status":"bookable","duration":3600,"price_min":1000,"price_max":1000}}
]}
```

После выбора `staff_id=1521566` и `datetime=2026-04-26T23:00:00+05:00` (запрос #82) ответ сужается до одной услуги — `7849893` (потому что Корт 5 — открытый, и под него подходит только аренда открытого).

### Запрос B2: POST /api/v1/booking/search/staff

**Зачем:** для текущего фильтра — список staff_id, у которых `is_bookable: true|false` и какие диапазоны цен.
**Body пример:** `{"context":{"location_id":521176},"filter":{"datetime":null,"records":[{"staff_id":null,"attendance_service_items":[]}]}}`.
**Response:** массив всех 16 staff_id с булевым `is_bookable`. После сужения по datetime+service остаются только релевантные.

**Полезный неочевидный сценарий (запрос #102):** тут можно проверить, доступен ли слот на конкретного staff: в request body передаём `datetime`, `attendance_service_items: [{type:service, id:7849893}]`, `staff_id: null` — и получаем массив всех staff и булевый `is_bookable`. Это **дешёвый probe для "слот ещё открыт?"** без попытки реально бронить.

### Запрос B3: POST /api/v1/booking/search/dates

**Зачем:** массив дат `date_from..date_to` с признаком `is_bookable`.
**Body:** `{"context":{"location_id":521176},"filter":{"date_from":"2026-04-24","date_to":"9999-01-01","records":[{"staff_id":1521566,"attendance_service_items":[]}]}}`.
**Response:** длинный массив `{"date":"2026-04-26","is_bookable":true|false}`.
**Используется для определения окна открытия записи** — это, видимо, единственный способ узнать «сколько дней вперёд открыто».

### Запрос B4: POST /api/v1/booking/search/timeslots

**Зачем:** для конкретной даты — массив timeslot-ов с временем (`time: "23:00"`, `datetime: "2026-04-26T23:00:00+05:00"`, `is_bookable: true`).
**Body:** `{"context":{"location_id":521176},"filter":{"date":"2026-04-26","records":[{"staff_id":1521566,"attendance_service_items":[]}]}}`.
**Response пример:**
```json
{"data":[{"type":"booking_search_result_timeslots","id":"4300c5977bee4419f8d6fd734eb8180f","attributes":{"datetime":"2026-04-26T23:00:00+05:00","time":"23:00","is_bookable":true}}]}
```

**Заметка:** `id` (хеш) у timeslot — детерминирован от location+staff+datetime, в `book_record` его передавать не надо.

**Идемпотентность:** все search/* — read-only, идемпотентны, никаких побочных эффектов.

---

## Группа C. Подготовка контактного шага

### Запрос C1: POST /api/v1/booking/locations/521176/attendances/calculate

**Когда:** сразу после того, как пользователь нажал «продолжить» на выбранный слот и оказался на форме ввода имени/телефона.
**Зачем:** server-side калькуляция стоимости и валидация комбинации (staff + service + datetime). Ответ возвращает `attendance_id` (UUID-хеш `fc43efa0b924b8f0716b5179bec0edd1`), но он, судя по `/book_record` payload, дальше **не передаётся**.

**Метод/URL:**
```
POST https://b551098.alteg.io/api/v1/booking/locations/521176/attendances/calculate
```
**Headers:** обычные, **без** signature/context.
**Request body (#112):**
```json
{
  "datetime": "2026-04-26T23:00:00+05:00",
  "records": [
    {
      "staff_id": 1521566,
      "attendance_service_items": [{"type": "service", "id": 7849893}]
    }
  ]
}
```
**Response (200):** объект с `data.attributes.datetime`, `data.relationships.records`, `included` массив с `service_items` и `available_staff_ids`.

**Ключевое:** ответ содержит `available_staff_ids: [1521566]` — подсказка, что staff остался доступен. Это ещё один cheap probe.

**Идемпотентен.** Не оставляет побочных эффектов (никакой soft-резерв на стороне сервера). Несколько вызовов calculate подряд — нормально, каждый вернёт новый UUID, но никаких состояний не создаст.

### Запрос C2: GET /api/v1/countries

Справочник стран для валидатора телефона (50 KB JSON). Захардкоден на фронте. Backend для нашего флоу не использует. Игнорируем.

---

## Группа D. **Создание брони** (горячий путь)

### Запрос D1: POST /api/v1/book_record/521176 — единственный требуемый POST

**Когда происходит:** пользователь нажал «Записаться» на форме контактных данных. Это **единственный мутирующий запрос во всём флоу**.

**Зачем нужен:** создать запись (бронь). Возвращает `record_id` и `record_hash`.

**Метод и URL:**
```
POST https://b551098.alteg.io/api/v1/book_record/521176
```
(`521176` в URL = location_id; bookform_id передаётся в body.)

**Query-параметры:** нет.

**Минимально достаточный набор headers (после провокации #4):**
```
Authorization: Bearer gtcw***0sadh        # СЕКРЕТ, ALTEGIO_BEARER_TOKEN из .env
Content-Type: application/json
accept: application/json, text/plain, */*
```
Этого хватит. Всё, что ниже (signature/context, altegio-application-*, sentry-*) — НЕ обязательно на сервере. Сохранено как реальный снимок из HAR для исторической ясности.

**Request headers (значимые) — РЕАЛЬНЫЕ ЗНАЧЕНИЯ ИЗ HAR:**
```
accept: application/json, text/plain, */*
accept-language: ru-RU
content-type: application/json
content-length: 666
origin: https://b551098.alteg.io
referer: https://b551098.alteg.io/company/521176/create-record/record?o=m1521566s7849893d2626042300
user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36

x-altegio-application-name: client.booking
x-altegio-application-platform: angular-18.2.13
x-altegio-application-version: 199620.2b0fce8b
x-altegio-application-action:               (пусто)

x-app-signature: 91146a3c401f8fcb7c53196ef6f7d1fc01d236f53fa0beb035787c355fc348c8
x-app-client-context: yqkTRyoePUlM96wD:yVG4CM93YsNbMWaJoPSSalkUjUxHqOmtoVmPP2rGvQXJ5L8dZNdTavDeQoCb2qE51fJqdjzTTTD0McGF9eIDGMbEiRjTbi7pu+eS529fYWqbHtQkCK2JuyTMmBnJnJql/W25dsYooRrhgBFetDmTajXJgj6UTzxu78nhLQFYUaJSr/SDNiX9gqJmpfvCdhvGjpmQHEE8LS3f9n30lLv+1Nin5bSiu0ggex4EKv1yREVZ66Fk0Kw2nICALCiOgpBVxGtWGmstLpRCjt8fh1lS3p2PCgmfsvkrFxr7OMOjllhsVK0V9Tt1OtaSMwiaYVhJmA7VWMWsC+FMS0q63PW3ka/w6UEGu863JZVQHL7jy01YmCR5g0TMrHsbOaRifpuL0/spWf1+nV0OkXxll1USutd0ZRz3K3ussyUSDsGcKY+RZrVX6kvf6M4XHY754Na/3r0SaZESJu9X1frPN0Gk4yV7xsQrpu+FnY3bS+9O7cnEVNpR/J3L
x-app-validation-token:                     (пусто)

baggage: sentry-environment=...               (Sentry)
sentry-trace: ...                             (Sentry)

# НЕТ Cookie
# НЕТ Authorization
# НЕТ X-CSRF-*
```

**Это единственный запрос за весь флоу, на котором `x-app-signature` и `x-app-client-context` не пустые.** Все search/* и calculate шлют их пустыми и сервер не возражает.

Анализ ходом мысли (исторический research-лог, ДО провокации #4):
- `x-app-signature: 91146a3c...` — 64 hex-символа = 32 байта. Похоже на SHA-256 hex от чего-то.
- `x-app-client-context: yqkTRyoePUlM96wD:yVG4CM9...` — формат `<16-char prefix>:<base64-ish blob ~500 chars>`. 16-символьный префикс выглядит как IV/nonce, остальное — base64 (там есть `+`, `/`, заглавные/строчные буквы и цифры). Вероятно, симметрично шифрованный JSON с fingerprint браузера + timestamp + параметры запроса. Ключ хранится в JS бандле (`199620.2b0fce8b` — версия). Точную схему генерации без анализа JS не определить.
- Изначально предполагалось: эти заголовки реализуют антибот-защиту Altegio и без них POST будет отвергнут.

**ОБНОВЛЕНИЕ (провокация #4, 2026-04-24, DONE ✓):** проверено вручную через cURL/Postman повтором POST с минимальным набором заголовков. Результат:

- **Достаточно одного «секретного» заголовка `Authorization: Bearer <token>` плюс стандартные** (`Content-Type: application/json`, `Accept: application/json, text/plain, */*`).
- **`x-app-signature` и `x-app-client-context` НЕ обязательны на сервере.** Их можно полностью опускать — POST проходит и возвращает 201 с тем же shape ответа.
- **Bearer token статичный** (по утверждению пользователя — не ротируется между сессиями). Не зависит от cookie, не зависит от bootstrap-запросов, не зависит от версии JS-бандла.
- **`x-altegio-application-name`/`-platform`/`-version`:** статус неясен — отдельно не провоцировались. Скорее всего тоже не обязательны (раз Bearer достаточно), но не подтверждено эмпирически. Безопасный путь: пока шлём те же значения, что в HAR, в реверс не лезем.
- **Bearer token (маскированно):** `gtcw***0sadh`. Полный токен — СЕКРЕТ, **не коммитится в репозиторий и не появляется в публичной документации**. Хранится в `.env` как переменная `ALTEGIO_BEARER_TOKEN`, добавляется в `.env.example` как пустая заглушка. Откуда токен берётся изначально — отдельный вопрос (вероятно, авторизация в личном кабинете Altegio выдаёт его и он живёт долго; провокация по ротации не делалась).

**Архитектурное следствие:** реверс JS-бандла и/или headless Chromium для генерации `x-app-signature`/`x-app-client-context` **НЕ требуется**. Hot-path можно реализовать pure `httpx` (или любой синхронный HTTP-клиент) с одним статичным Bearer-токеном из env. Это радикально упрощает Phase 2.

Статичные headers: `x-altegio-application-name`, `-platform`, `-version`, `accept`, `user-agent`, `origin`, `accept-language`.
Динамические (требуют генерации):
- `referer` — содержит UX-state в query (`o=m1521566s7849893d2626042300` — слепок выбранного master/service/date в формате `m{staff}s{service}d{YY}{MM}{DD}{HH}{MM}` = `m1521566 s7849893 d 26 26 04 23 00`). Возможно, не валидируется — провокация.
- `content-length` — стандартно от тела.
- `x-app-signature` и `x-app-client-context` — антибот-токены, как описано выше.
- `sentry-trace`, `baggage` — Sentry, можно опустить.

**Request body (полное тело из HAR):**
```json
{
  "fullname": "Роман",
  "surname": null,
  "patronymic": null,
  "phone": "77026473809",
  "email": "romer533@mail.ru",
  "comment": "",
  "custom_fields": {},
  "is_newsletter_allowed": null,
  "is_personal_data_processing_allowed": null,
  "appointments": [
    {
      "services": [7849893],
      "staff_id": 1521566,
      "datetime": "2026-04-26T23:00:00",
      "chargeStatus": "",
      "custom_fields": {},
      "id": 0,
      "available_staff_ids": [1521566]
    }
  ],
  "bookform_id": 551098,
  "isMobile": false,
  "notify_by_sms": 1,
  "referrer": "",
  "is_charge_required_priority": true,
  "is_support_charge": false,
  "appointments_charges": [
    {"id": 0, "services": [], "prepaid": []}
  ],
  "redirect_url": "https://b551098.alteg.io/company/521176/success-order/{recordId}/{recordHash}"
}
```

Комментарии по каждому полю:
| Поле | Откуда берётся | Обязательно? |
|------|----------------|--------------|
| `fullname` | из формы (только имя, без фамилии) | да (как минимум так в HAR) |
| `surname`, `patronymic` | null — клуб не настроил эти поля как обязательные (см. ответ /company: `is_surname_field_enabled: false`) | нет |
| `phone` | из формы, формат `7XXXXXXXXXX` (11 цифр без `+`, `(`, `-`); в HAR: `77026473809` | да |
| `email` | из формы (`romer533@mail.ru`); в /company `booking_email_required: false` — провокация требуется чтобы убедиться | возможно нет |
| `comment` | пустая строка (поле есть, но не заполнено) | нет (`booking_comment_required: false`) |
| `custom_fields` | `{}` — клуб не настроил | нет |
| `is_newsletter_allowed` / `is_personal_data_processing_allowed` | null — UX-чекбоксы, не обязательны | нет |
| `appointments[0].services` | `[service_id]` (массив, поддерживает несколько услуг) | да |
| `appointments[0].staff_id` | id корта | да |
| `appointments[0].datetime` | **ВАЖНО:** в формате `2026-04-26T23:00:00` БЕЗ суффикса timezone, в локальном времени клуба (Astana). НЕ ISO-8601 с offset. Несовпадает с тем, что возвращают search/timeslots (там `+05:00`) | да |
| `appointments[0].chargeStatus` | `""` (для случаев предоплаты, у нас не используется) | строкой пустой |
| `appointments[0].custom_fields` | `{}` | нет |
| `appointments[0].id` | `0` (новая бронь; видимо, для редактирования передавался бы существующий) | `0` для новой |
| `appointments[0].available_staff_ids` | из ответа `/attendances/calculate` или из `search/staff`. В HAR: `[1521566]`. Возможно, не валидируется server-side (нужна провокация — пустой массив, лишние id) | передавать тот же staff_id |
| `bookform_id` | `551098` (захардкожен в URL `b551098.alteg.io`, b + bookform_id) | да |
| `isMobile` | `false` (от UA-detection фронта) | возможно нет |
| `notify_by_sms` | `1` — клиент хочет получить SMS-напоминание (`sms_enabled: true` в /company; не путать с phone-confirmation, которое выключено) | можно `0` |
| `referrer` | `""` — реферал, бесполезен | пустой |
| `is_charge_required_priority` | `true` (фронт-флаг, видимо влияет на UX prepaid) | да копируем |
| `is_support_charge` | `false` (есть ли поддержка предоплаты) | да копируем |
| `appointments_charges` | `[{"id":0,"services":[],"prepaid":[]}]` — заглушка по prepaid, у нас prepaid forbidden | да копируем |
| `redirect_url` | `"https://b551098.alteg.io/company/521176/success-order/{recordId}/{recordHash}"` — ссылка для callback после оплаты, плейсхолдеры серверу нужны для подстановки в SMS/email | формат как в HAR |

**Response (success):**
- Статус: **`201 Created`** (важно: не 200).
- Headers: `content-type: application/json`, **`Set-Cookie: отсутствует`**, `x-request-id`.
- Тело (массив с одним элементом):
```json
[{
  "id": 0,
  "record_id": 645268016,
  "record_hash": "554f7a6a693197209816116ea42f3b09",
  "record": {
    "id": 645268016,
    "services": [{
      "id": 7849893,
      "title": "Аренда открытого корта (1 час)",
      "cost": 2500,
      "price_min": 2500,
      "price_max": 2500,
      "discount": 0,
      "amount": 1,
      "seance_length": 3600,
      "abonement_restriction": 0,
      "prepaid_settings": { "status": "forbidden", ... }
    }],
    "company": {
      "id": 521176,
      "title": "Daulet Tennis Academy",
      "city": "Астана",
      "address": "ул. Кордай, 6",
      "timezone": 6,                    // (sic, 6, не 5)
      "currency_short_title": "₸",
      "allow_change_record_delay_step": 10800,
      "allow_delete_record_delay_step": 18000,
      ...
    },
    "clients_count": 1,
    "date": "2026-04-26 23:00:00",
    "datetime": "2026-04-26T23:00:00+0500",
    "create_date": "2026-04-24T18:01:47+0500",
    "deleted": false,
    "attendance": 0,
    "length": 3600,
    "notify_by_sms": 1,
    "notify_by_email": 1,
    "online": true,
    "staff": {
      "id": 1521566,
      "name": "Корт №5",
      "specialization": "Открытый",
      "position": {"id": 180845, "title": "Открытый", "services_binding_type": 0},
      "rating": 4.38
    },
    "paid_amount": 0,
    "allow_delete_record": true,
    "allow_change_record": true,
    "is_confirmation_needed": false
  }
}]
```

Критичные поля для следующих шагов:
- `record_id` (`645268016`) — числовой ID брони. Используется в URL `/attendances/{record_id}` для просмотра/отмены.
- `record_hash` (`554f7a6a693197209816116ea42f3b09`) — секретный хеш, нужен как query-param `?hash=...` для запросов на просмотр чужим клиентом.
- `is_confirmation_needed: false` — финальная гарантия, что SMS-подтверждения после создания не требуется. Бронь сразу валидна.

**Response (errors), которые удалось спровоцировать:**
- В HAR не наблюдалось — пользователь сделал успешную бронь с первой попытки. **Нужны провокации** (см. блок "Открытые вопросы").

**Заметки:**
- Идемпотентность: **не проверена**. Скорее всего НЕ идемпотентен — повторный POST с тем же телом создаст дубль или вернёт business-error с конкретным кодом ("слот занят"). Нужна провокация.
- Rate-limit: **не проверен**. В HAR никаких 429. **Нужна провокация** (5+ параллельных POST).
- Зависимость от cookie: **в HAR нет ни одной cookie во всём флоу** — значит сервер не требует state, выставленного предыдущими GET. Это сильный аргумент в пользу того, что POST можно стрелять «холодно», без bootstrap-разогрева. **И после провокации #4 это подтвердилось полностью:** антибот-токены `x-app-signature`/`x-app-client-context` НЕ требуются, хватает статичного `Authorization: Bearer ...`. То есть hot-path действительно «холодный» — никакой prewarm, никакой headless-Chromium, никакого реверса JS-бандла.

---

## Группа E. Пост-брони (после 201)

После успешного `book_record` фронт делает 3 запроса (для отрисовки success-страницы):

### Запрос E1: POST /api/v1/booking/search/staff — снова, с пустым фильтром. Бесполезно для нас.
### Запрос E2: POST /api/v1/booking/search/dates — снова. Бесполезно.

### Запрос E3: GET /api/v1/booking/locations/521176/attendances/{record_id}/?hash={record_hash}&bookform_id=551098&include[]=location.map

**Зачем:** получить детали брони для отображения подтверждения. Нам полезно как **способ проверки, что бронь действительно создана** (для confirm/double-check логики).

**URL:** `https://b551098.alteg.io/api/v1/booking/locations/521176/attendances/645268016/?hash=554f7a6a693197209816116ea42f3b09&bookform_id=551098&include[]=location.map`

**Response (200):** JSON:API-формат с `data.attributes`:
- `id: "645268016"`, `hash: "554f7a6a..."`, `token: "RPDQO"` — короткий человекочитаемый код брони (для клуба).
- `datetime: "2026-04-26T23:00:00+0500"`, `duration: 3600`, `attendance_status: 0`.
- `is_delete_record_allowed: true`, `is_change_record_allowed: true` — можно отменять/менять (но за `allow_delete_record_delay_step` секунд до начала).
- `is_confirmation_needed: false`, `is_acceptance_payment: false`, `is_prepaid: false`.

В `included` — staff (`Корт №5`, position "Открытый"), service (`Аренда открытого корта`, cost 2500), location (адрес, телефоны, координаты, ссылки на соцсети).

---

## Сводка

### Горячий путь (что нужно стрелять в T−0)

**Минимально достаточно одного запроса** — `POST /api/v1/book_record/521176` — при условии, что `service_id`, `staff_id`, `datetime`, `customer.fullname`, `customer.phone` уже известны заранее (что в нашем случае — да, мы их знаем для всех воскресных слотов 23:00 на нужном корте).

То есть в момент T−0:
1. **POST /api/v1/book_record/521176** с body вида:
   ```json
   {
     "fullname": "...",
     "phone": "7XXXXXXXXXX",
     "email": "...",
     "appointments":[{"services":[7849893],"staff_id":<court>,"datetime":"YYYY-MM-DDTHH:MM:SS","id":0,"chargeStatus":"","custom_fields":{},"available_staff_ids":[<court>]}],
     "bookform_id": 551098,
     "notify_by_sms": 1,
     "is_charge_required_priority": true,
     "is_support_charge": false,
     "appointments_charges":[{"id":0,"services":[],"prepaid":[]}],
     "custom_fields":{},
     ...остальные поля как в HAR
   }
   ```
   и headers — **достаточно одного секретного:** `Authorization: Bearer <ALTEGIO_BEARER_TOKEN>` + стандартные `Content-Type: application/json`, `accept: application/json, text/plain, */*`. Без cookie, без `x-app-signature`, без `x-app-client-context`. Подтверждено провокацией #4.

Никаких прогревочных GET-запросов сервер НЕ требует (cookies во всём флоу — ноль), антибот-токены не требуются. Единственная «секретная» зависимость — статичный Bearer из env.

**Опционально ДО окна (за 30s):**
- `POST /api/v1/booking/search/timeslots` с фильтром `date=<sunday>`, `staff_id=<court>` — узнать `is_bookable` в realtime (cheap probe). Если возвращается `is_bookable: false` — значит окно ещё не открылось, ждём.
- `POST /api/v1/booking/locations/521176/attendances/calculate` — финальная валидация комбинации (вернёт `available_staff_ids`); побочных эффектов нет.

**Что нужно иметь к T−0 (assumed pre-known):**
- `service_id = 7849893` (открытый корт), `bookform_id = 551098`, `location_id = 521176` — статика клуба.
- Список возможных `staff_id` уличных кортов: `1521562, 1521564, 1521565, 1521566, 1521567` (5 шт) — из `/book_staff`. Обновлять раз в день/неделю.
- Имя клиента, телефон в формате `7XXXXXXXXXX`, email.
- **`ALTEGIO_BEARER_TOKEN`** — статичный, из env. Это единственный «секрет», который нужно держать. Раньше «ключевой неизвестной» считались `x-app-signature` / `x-app-client-context`, но провокация #4 показала: они не нужны.

Cookie/сессии заранее получать НЕ нужно — их просто нет. Bearer-токен живёт долго и не требует rotation в hot-path.

### Авторизация / CSRF

- **Cookie:** ни одной во всём флоу. Сервер не выставляет, фронт не шлёт. Сессии нет.
- **Authorization header:** **обязателен на `POST /book_record/...`.** Формат: `Authorization: Bearer <ALTEGIO_BEARER_TOKEN>`, например `Bearer gtcw***0sadh` (полный токен — секрет, в `.env`). Токен **статичный** (по утверждению пользователя — не ротируется), хранится на стороне клиента долго. В HAR его не видно, потому что HAR снимался из браузера после авторизации в личном кабинете Altegio (видимо, фронт хранит токен в `localStorage`/`sessionStorage` и приклеивает к запросам через interceptor — но в HAR Chrome его не показал, что и сбило с толку первоначальный анализ).
- **CSRF-токен:** отсутствует.
- **Антибот-защита (исторический контекст):** изначально считалось, что `x-app-signature` (32-байт hex, похоже на HMAC/SHA-256) и `x-app-client-context` (формат `<16-char-prefix>:<~500-char base64>`, AES-CBC/GCM зашифрованный JSON) — обязательны для POST. **Провокация #4 опровергла это:** сервер принимает POST без них, если есть валидный Bearer. Это, вероятно, фронтовая антибот-телеметрия (для аналитики/Sentry), а не серверная проверка. Шлются они ТОЛЬКО на `book_record` POST из браузера; на сервере при наличии Bearer не валидируются.
- **`x-altegio-application-*` headers (name/platform/version):** статичные строки, идентифицирующие фронт. Статус **не подтверждён эмпирически** (отдельной провокации не было). Гипотеза: тоже не обязательны на сервере при наличии Bearer. Безопасный путь — пока шлём, но Phase 2 может попробовать опустить.
- **Если опустить Origin/Referer:** не проверено отдельно. Скорее всего сервер не валидирует при Bearer-авторизации (типичный pattern — same-origin checks отключают для bearer-flow).

**Можно ли разогреть заранее:** **не нужно вообще.** Bearer статичный, лежит в env, в момент T−0 просто прилеплен к POST. Hot-path реализуется чистым `httpx` без headless-браузера и без реверса JS. Это было главное архитектурное решение, и провокация #4 закрывает его в пользу простого пути.

### Подтверждение по SMS / Captcha

- **SMS-подтверждение телефона перед созданием брони: НЕТ.**
  - В `/bookform/551098/`: `phone_confirmation: false`.
  - В `/company/521176`: `phone_confirmation: false`, `push_notification_phone_confirm: 1` (это UX-флаг, не блокирующий).
  - В ответе `/book_record`: `is_confirmation_needed: false`.
  - В `/booking/forms/551098/security_levels/`: пустой массив (security-уровни клуб не настраивал).
- **Captcha: не наблюдалась** ни на одном этапе. Никаких reCAPTCHA / hCaptcha / cloudflare challenges в HAR не было.
- **SMS-уведомление ПОСЛЕ создания:** да, опционально, контролируется `notify_by_sms: 1` в payload. Это исходящее уведомление клиенту, не блокирующее.

### Поведение на закрытом слоте (T < открытие окна)

**Не наблюдалось в HAR** — пользователь успешно забронировал валидный слот. Гипотеза по `search/dates`: сервер возвращает `is_bookable: false` для дат за горизонтом (>= 4 дня вперёд). Если попытаться напрямую `book_record` для такой даты — ответ неизвестен (404? 422? пустое тело? business-error в массиве?).

**Провокация требуется обязательно.** См. "Открытые вопросы".

### Rate-limiting

**Не наблюдался в HAR** — все 35 API-запросов прошли с 200/201. `x-ratelimit-*` headers сервер НЕ возвращает. `Retry-After` не возвращает.

**Провокация требуется обязательно.** Запустить N≥5 одновременных POST `/book_record` на разные тестовые слоты и зафиксировать поведение.

### Прочие наблюдения

- **Версия API:** `/api/v1/...` для всех эндпоинтов. Sentry-релиз бандла фронта: `199620.2b0fce8b`.
- **Smesh API дизайнов:** часть эндпоинтов в JSON:API формате (`/booking/search/*`, `/booking/locations/.../attendances/*`, `/booking/forms/.../custom_fields`), часть — старый ad-hoc формат (`/bookform`, `/company`, `/book_services`, `/book_staff`, `/book_record`). Видно, что Altegio мигрирует — нужно быть готовым.
- **WebSocket / SSE:** нет. Весь флоу — обычный REST.
- **HTTP/2 (h2):** да, видно по pseudo-headers `:authority`, `:method` в HAR. Это упрощает мультиплексирование запросов (для параллельной стрельбы).
- **DNS / CDN:** домен `b551098.alteg.io` — общая платформа Altegio. Поддомен `b{bookform_id}` явно генерируется автоматически. Тот же бекенд обслуживает тысячи клубов.
- **Sentry:** `errors.alteg.io/api/38/envelope/` — фронт активно репортит ошибки в Sentry. Это значит, что все антибот-обходы будут видны Altegio в их dashboard. Стоит учитывать репутационный риск.
- **GA:** Google Analytics активен (`G-R10GHC8GD3`).
- **Internal tracking:** `tracks.alteg.io/api/v1/track` — собственная аналитика Altegio со счётчиками UX-кликов. Её можно не вызывать, никаких побочных эффектов на бронь.
- **Часовой пояс в payload датавремени:** в `book_record` body `datetime` шлётся БЕЗ timezone-суффикса (`2026-04-26T23:00:00`), в локальном времени клуба. В URL referer-параметре `d2626042300` — это `26 26 04 23 00` (год без века? день месяц + время). Странный формат, возможно `d{ddYY MMHHmm}` или `d{day}{year_short}{month}{hour}{minute}` — нужно перепроверить генерацию на других слотах. Вероятно, фронтовый артефакт, серверу не нужен.
- **`x-altegio-application-action`** — указывает контекст UX-страницы (`""`, `"company"`, `"company.new-success-order"`). Похоже на телеметрию, серверу безразлично.

---

## Открытые вопросы для PO (важно)

### A. Провокации

**Статус провокаций (обновлено 2026-04-24):**

| #  | Провокация                              | Статус       | Блокер для Phase 2? |
|----|-----------------------------------------|--------------|---------------------|
| 1  | Закрытый слот (T < открытие окна)       | TODO         | nice-to-have        |
| 2  | Занятый слот                            | TODO         | nice-to-have        |
| 3  | Дубликат (parallel POST, тот же слот)   | TODO         | nice-to-have        |
| 4  | Без антибот-токенов                     | **DONE ✓**   | —                   |
| 5  | Без Origin/Referer                      | TODO         | nice-to-have        |
| 6  | Параллелизм на разных слотах            | TODO         | nice-to-have        |
| 7  | search/timeslots до открытия окна       | TODO         | nice-to-have        |
| 8  | Точный момент открытия окна             | TODO         | nice-to-have        |

После провокации #4 ни один из оставшихся пунктов **не блокирует старт Phase 2** — все они нужны для tuning engine retry-логики (правильные коды ошибок → правильный backoff), но горячий путь работает уже сейчас. Их можно делать параллельно с разработкой клиента.

---

1. **Закрытый слот (T < открытие окна).** *(nice-to-have)* Через cURL/Postman повторить `POST /book_record` с `datetime` через 4+ дня вперёд (за горизонтом 3 дней). Зафиксировать: статус (404? 422? 200 с error в теле?), точное тело ответа, есть ли `error_code` / `meta`. Влияет на retry-стратегию engine — нужно различать "слот ещё не открылся, повторять каждые 200ms" от "слот занят навсегда, бросаем".

2. **Занятый слот.** *(nice-to-have)* Сразу после создания брони повторить тот же POST (тот же staff_id, datetime). Что вернёт? 409? 422? 200 с `success: false`? Точный shape ответа — для классификатора ошибок engine.

3. **Дубликат.** *(nice-to-have)* Запустить два одинаковых POST с интервалом 50ms (тот же staff/datetime/phone). Получится две брони, одна+ошибка, или обе пройдут? Влияет на стратегию idempotency-key.

4. **Без антибот-токенов.** **DONE ✓** *(2026-04-24)* — повторили POST с минимальным набором headers (`Authorization: Bearer <token>` + `Content-Type` + `accept`), опустив `x-app-signature` и `x-app-client-context`. **Прошло. 201 Created, тот же shape ответа.** Bearer-токен оказался статичным, антибот-headers серверу не нужны. Реверс JS-бандла и headless Chromium **сняты с дорожной карты**. Подробнее — в разделах "Группа D" и "Сводка → Авторизация".

5. **Без Origin/Referer.** *(nice-to-have)* То же, без этих headers. Скорее всего пройдёт (Bearer-flow обычно не требует), но эмпирически не подтверждено.

6. **Параллелизм.** *(nice-to-have)* N ∈ {3, 5, 10} одновременных POST на РАЗНЫЕ тестовые слоты (можно с разными staff_id). Все пройдут? Какой-то порог 429? `Retry-After`? Влияет на выбор стратегии "single-shot vs. shotgun" в hot-path.

7. **search/timeslots до открытия окна.** *(nice-to-have)* Запросить `search/dates` с `date_from = T+4` (явно за горизонтом). Вернёт массив с `is_bookable: false` или сразу пустой? Можно ли поллить именно через search/timeslots с конкретной датой и заскаутить момент перехода?

8. **Точный момент открытия окна.** *(nice-to-have, для калибровки таймера)* Запустить `search/dates` каждые 5 секунд в окрестности `06:55:00..07:05:00 Asia/Almaty` целевого дня. Зафиксировать timestamp перехода `is_bookable: false → true`.

### B. Архитектурные вопросы по результатам HAR

1. **Реверсинг `x-app-signature` / `x-app-client-context` vs. headless-браузер.** **РЕШЕНО провокацией #4 в пользу третьего варианта — pure httpx с Bearer-токеном.** Историческая развилка (оставлено для контекста):
   - ~~**Реверс JS:**~~ скачать бандл `199620.2b0fce8b`, найти функции генерации токенов, переписать на Python. — **Не нужен.**
   - ~~**Headless-браузер (Playwright):**~~ запускать headless Chromium, делать `fetch` через `page.evaluate`. — **Не нужен.**
   - ~~**Гибрид:**~~ длящийся headless-браузер + `page.evaluate` в T−0. — **Не нужен.**
   - **Текущий путь:** `httpx.AsyncClient`, статичный Bearer-токен из env, один POST. Latency = чистый network round-trip, плюс serialize JSON — единицы миллисекунд.

2. **«Холодная» стрельба.** Cookies нет, сессии нет, антибот-токенов на сервере нет — значит prewarm не нужен **ни в каком виде**. Bearer статичный, лежит в `.env` всё время. Hot-path действительно «однострочный»: один POST в T−0. Открытый под-вопрос: **TTL Bearer-токена**. Пользователь утверждает, что не ротируется — но эмпирически "сколько живёт токен" мы не знаем. Стратегия: мониторить 401 на проде, при появлении — runbook "обновить токен из браузера → положить в env". Это не блокер для Phase 2.

3. **Параллелизм N запросов.** Если rate-limit либеральный, стратегия "N одинаковых POST с разных IP в момент T−0" даёт буст. Если жёсткий — наоборот, лучше одиночный POST с супер-точным таймингом. Зависит от провокации #6.

4. **Версия API.** Половина эндпоинтов в JSON:API формате (новый), половина в ad-hoc (старый). Altegio явно мигрирует. Стоит периодически перепроверять, не сменился ли формат (особенно `book_record`).

5. **Sentry-телеметрия.** Фронт шлёт всё в Sentry Altegio. Если стрелять с того же `User-Agent`/`origin`, Altegio в принципе может обнаружить аномальный паттерн (1 POST в неделю на одну и ту же бронь в одно и то же время) и забанить. Стоит обсудить — насколько мы готовы быть "видимыми" в Sentry клуба и сети Altegio.

6. **CLAUDE.md-расхождение про город.** В корневом `CLAUDE.md` упоминается Atyrau — реально клуб в Astana (Нур-Султан). Нужно поправить в отдельном PR.

### C. Чек-лист готовности Phase 0

- [x] Перечислен горячий путь (1 запрос: `POST /book_record/{location_id}`).
- [x] Понятно, какие токены/cookies нужны: cookies — НИКАКИЕ; токен — статичный `Authorization: Bearer <ALTEGIO_BEARER_TOKEN>` из env. Антибот-токены `x-app-signature` / `x-app-client-context` на сервере **не валидируются** (провокация #4 ✓).
- [x] Понятно, требуется ли SMS / captcha: нет. Captcha не наблюдалась ни на одном этапе, в том числе при минимальном наборе headers (провокация #4).
- [x] Архитектурное решение по обходу антибота: **pure `httpx`, без headless Chromium и без реверса JS-бандла.** Закрыто провокацией #4.
- [ ] **Nice-to-have, не блокер:** что вернёт API на запрос ДО открытия окна (провокация #1). Нужно для retry-классификатора.
- [ ] **Nice-to-have, не блокер:** rate-limit при N параллельных запросов (провокация #6). Нужно для выбора single-shot vs. shotgun.
- [ ] **Nice-to-have, не блокер:** идемпотентность booking-POST (провокация #3). Нужно для idempotency-key стратегии.
- [ ] **Nice-to-have, не блокер:** точное поведение на занятом слоте (провокация #2). Нужно для shape ошибки.

**Статус Phase 0: фактически закрыта для перехода в Phase 2.** Горячий путь подтверждён, секрет (Bearer) идентифицирован, антибот-вопрос снят. Оставшиеся провокации (#1, #2, #3, #5, #6, #7, #8) переведены в категорию "tuning движка retry-логики" и могут выполняться параллельно с разработкой Phase 2 клиента — они уточняют поведение на edge-cases, но не меняют архитектуру.
