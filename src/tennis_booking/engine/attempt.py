from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

import structlog

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioClient,
    AltegioTransportError,
    BookingResponse,
    TimeSlot,
)
from tennis_booking.common.clock import Clock
from tennis_booking.common.tz import ALMATY
from tennis_booking.config.schema import GracePollingConfig
from tennis_booking.obs.telegram import (
    TelegramNotifier,
    disabled_notifier,
    format_lost_message,
    format_timeout_message,
    format_win_message,
)
from tennis_booking.persistence import BookedSlot, BookingStore
from tennis_booking.persistence.models import PROFILE_NAME_RE, SCHEMA_VERSION

from .codes import CONFIG_ERROR_CODES, NOT_OPEN_CODES, SLOT_TAKEN_CODES

__all__ = [
    "AttemptConfig",
    "AttemptPhase",
    "AttemptResult",
    "AttemptStatus",
    "BookingAttempt",
]

_logger = structlog.get_logger(__name__)

AttemptStatus = Literal["won", "lost", "timeout", "error"]
AttemptPhase = Literal["window", "poll"]

_TIGHT_LOOP_LEAD_S = 1.0
_TIGHT_LOOP_STEP_S = 0.001
_PREARM_MIN_BUDGET_S = 0.1
_PREARM_TAIL_GUARD_S = 0.1
_PER_SHOT_TAIL_GUARD_S = 0.1
_PER_SHOT_MIN_TIMEOUT_S = 0.2

_GRACE_SEARCH_TIMEOUT_S = 5.0
_GRACE_PER_SHOT_TIMEOUT_S = 5.0

# Cloudflare per-IP rate-rule defensive backoff. Empirically (28.04 incident)
# CF starts blocking ~75% of POSTs when burst exceeds ~30 RPS — slow and cap
# at 2s so retry storms don't keep feeding the limiter.
_CF_BACKOFF_INITIAL_MS = 100
_CF_BACKOFF_CAP_MS = 2000
# Other transport (timeout, connect_error) — lighter; these are usually
# transient single-shot failures, not policy enforcement, so back off less.
_OTHER_TRANSPORT_BACKOFF_INITIAL_MS = 50
_OTHER_TRANSPORT_BACKOFF_CAP_MS = 500
# Don't bother re-firing a shot if less than this remains before global deadline —
# the per-shot timeout floor would just bounce immediately. Mark as final timeout.
_RETRY_DEADLINE_MIN_MARGIN_S = 0.1
_CLOUDFLARE_TRANSPORT_CAUSE = "cloudflare_challenge"


def _exponential_backoff_ms(retry_count: int, initial_ms: int, cap_ms: int) -> int:
    """retry_count is 1-based (first retry → initial_ms). Caps at cap_ms."""
    delay_ms: int = initial_ms * (2 ** (retry_count - 1))
    return min(cap_ms, delay_ms)


@dataclass(frozen=True)
class AttemptConfig:
    """Параметры одной попытки бронирования. Immutable — reused между prearm/fire/retry.

    court_ids — один или несколько courts: engine fan-out'ит параллельные shots
    на разные courts (по одному shot на court при len>1). Для legacy single-court
    — tuple из одного элемента, shots дублируются на тот же court согласно
    parallel_shots.
    """

    slot_dt_local: datetime
    court_ids: tuple[int, ...]
    service_id: int
    fullname: str
    phone: str
    profile_name: str
    email: str | None = None
    parallel_shots: int = 2
    not_open_retry_ms: int = 100
    not_open_deadline_s: float = 5.0
    global_deadline_s: float = 10.0
    prearm_lead_s: float = 30.0
    grace_polling: GracePollingConfig | None = None
    # Skip the fan-out fire if (slot_dt_local − now) < this. Altegio refunds free
    # cancellations only when more than 2h remain, so booking accidentally inside
    # that window strands money on a slot we cannot cleanly release. 0.0 disables
    # the guard (default for tests / legacy code paths). Production sets via
    # TENNIS_MIN_LEAD_TIME_HOURS env var → SchedulerLoop → AttemptConfig.
    min_lead_time_hours: float = 0.0
    # Optional opaque pool key for the shared poll-result cache. Plumbed from
    # ResolvedBooking.pool_name when the booking targets a court_pool. Used
    # purely for cache key construction + log readability — when None,
    # PollAttempt synthesises a fallback key from court_ids.
    pool_key: str | None = None
    # Cap initial fan-out width: pick min(this, len(court_ids)) random court_ids
    # from the pool. None = no cap (fire all court_ids — legacy behavior).
    # Defensive measure against Cloudflare's per-IP rate-rule (~30 RPS) that
    # blocks ~75% of POSTs in a 11-attempt × 7-court burst.
    max_parallel_shots: int | None = None

    def __post_init__(self) -> None:
        if self.slot_dt_local.tzinfo is None:
            raise ValueError("slot_dt_local must be timezone-aware")
        if self.slot_dt_local.tzinfo != ALMATY:
            raise ValueError(
                f"slot_dt_local must be in Asia/Almaty, got {self.slot_dt_local.tzinfo}"
            )
        if not isinstance(self.court_ids, tuple):
            raise ValueError(
                f"court_ids must be a tuple, got {type(self.court_ids).__name__}"
            )
        if not self.court_ids:
            raise ValueError("court_ids must contain at least one id")
        for cid in self.court_ids:
            if not isinstance(cid, int) or isinstance(cid, bool):
                raise ValueError(
                    f"court_ids entries must be integers, got {type(cid).__name__}"
                )
            if cid < 1:
                raise ValueError(f"court_ids entries must be >= 1, got {cid}")
        if len(set(self.court_ids)) != len(self.court_ids):
            raise ValueError(
                f"court_ids must be unique, got duplicates in {list(self.court_ids)}"
            )
        if self.service_id < 1:
            raise ValueError(f"service_id must be >= 1, got {self.service_id}")
        if not self.fullname.strip():
            raise ValueError("fullname must not be empty after strip")
        if not self.phone.strip():
            raise ValueError("phone must not be empty after strip")
        if not isinstance(self.profile_name, str) or not PROFILE_NAME_RE.fullmatch(
            self.profile_name
        ):
            raise ValueError(
                f"profile_name must match [a-z0-9_-]+, got {self.profile_name!r}"
            )
        if self.parallel_shots < 1:
            raise ValueError(f"parallel_shots must be >= 1, got {self.parallel_shots}")
        if self.not_open_retry_ms < 10:
            raise ValueError(f"not_open_retry_ms must be >= 10, got {self.not_open_retry_ms}")
        if self.not_open_deadline_s <= 0:
            raise ValueError(
                f"not_open_deadline_s must be > 0, got {self.not_open_deadline_s}"
            )
        if self.global_deadline_s <= self.not_open_deadline_s:
            raise ValueError(
                f"global_deadline_s ({self.global_deadline_s}) must be > "
                f"not_open_deadline_s ({self.not_open_deadline_s})"
            )
        if self.prearm_lead_s <= 0:
            raise ValueError(f"prearm_lead_s must be > 0, got {self.prearm_lead_s}")
        if self.min_lead_time_hours < 0.0:
            raise ValueError(
                f"min_lead_time_hours must be >= 0.0, got {self.min_lead_time_hours}"
            )
        if self.min_lead_time_hours > 168.0:
            raise ValueError(
                f"min_lead_time_hours must be <= 168.0 (1 week), got {self.min_lead_time_hours}"
            )
        if self.max_parallel_shots is not None and self.max_parallel_shots < 1:
            raise ValueError(
                f"max_parallel_shots must be >= 1 if set, got {self.max_parallel_shots}"
            )

    @property
    def effective_shots(self) -> int:
        """Эффективное число параллельных shots.

        Для pool (len(court_ids) > 1) — равно числу courts (по одному shot на
        court), capped by max_parallel_shots if set; `parallel_shots` silently
        ignored. Для legacy (len==1) — `parallel_shots` (дублирование на один
        и тот же court).
        """
        if len(self.court_ids) > 1:
            base = len(self.court_ids)
            if self.max_parallel_shots is not None:
                return min(base, self.max_parallel_shots)
            return base
        return self.parallel_shots


@dataclass(frozen=True)
class AttemptResult:
    """Исход одной попытки. Передаётся выше наверх (в loop/observability)."""

    status: AttemptStatus
    booking: BookingResponse | None
    duplicates: tuple[BookingResponse, ...]
    fired_at_utc: datetime | None
    response_at_utc: datetime | None
    duration_ms: float
    business_code: str | None
    transport_cause: str | None
    prearm_ok: bool
    shots_fired: int
    attempt_id: str = field(default="")
    phase: AttemptPhase | None = None


class BookingAttempt:
    """Одноразовый runner. Повторный run() → RuntimeError.

    State machine высокого уровня:
        (start) → validate_window → sleep_to_prearm → prearm
            → sleep_to_tight → tight_loop → fire (N shots)
            → retry_loop (not_open / transport) → (won | lost | timeout | error)
    """

    def __init__(
        self,
        config: AttemptConfig,
        client: AltegioClient,
        clock: Clock,
        store: BookingStore | None = None,
        rng: random.Random | None = None,
        *,
        cancel_duplicates_enabled: bool = True,
        notifier: TelegramNotifier | None = None,
    ) -> None:
        self._config = config
        self._client = client
        self._clock = clock
        self._store = store
        self._cancel_duplicates_enabled = cancel_duplicates_enabled
        self._notifier = notifier if notifier is not None else disabled_notifier()
        # Strong refs to fire-and-forget notification tasks. Without this, GC
        # may collect the task before it completes (Python docs warn that
        # asyncio.create_task only holds a weakref). Discarded via done-callback.
        self._notification_tasks: set[asyncio.Task[None]] = set()
        # SystemRandom for production (CSPRNG, no seedable state shared with code
        # under test); injectable for reproducible tests of subset selection.
        self._rng: random.Random = rng if rng is not None else random.SystemRandom()
        # Subset of court_ids to fan out over for THIS attempt — populated once at
        # _fire_and_retry start. Engine indexing logic (_spawn_shot, won_court_id
        # mapping) reads this, NOT config.court_ids — so a capped attempt fires
        # only on the chosen subset and never spills onto unselected courts.
        self._active_court_ids: tuple[int, ...] = config.court_ids
        self._used = False
        self._attempt_id = uuid.uuid4().hex
        log_bindings: dict[str, object] = {
            "attempt_id": self._attempt_id,
            "slot_dt_local": config.slot_dt_local.isoformat(),
            "dry_run": client.config.dry_run,
        }
        if len(config.court_ids) <= 7:
            log_bindings["court_ids"] = tuple(config.court_ids)
        else:
            log_bindings["court_id_primary"] = config.court_ids[0]
            log_bindings["court_count"] = len(config.court_ids)
        self._log = _logger.bind(**log_bindings)

    async def run(self, window_open_utc: datetime) -> AttemptResult:
        if self._used:
            raise RuntimeError("BookingAttempt.run() is single-shot; create a new instance")
        self._used = True

        start_utc = self._clock.now_utc()
        start_mono = self._clock.monotonic()
        self._log.info("scheduled", window_open_utc=window_open_utc.isoformat())

        if window_open_utc <= start_utc:
            self._log.warning(
                "window_passed",
                now_utc=start_utc.isoformat(),
                window_open_utc=window_open_utc.isoformat(),
            )
            return self._make_result(
                status="error",
                booking=None,
                duplicates=(),
                fired_at_utc=None,
                response_at_utc=None,
                start_mono=start_mono,
                business_code="window_passed",
                transport_cause=None,
                prearm_ok=False,
                shots_fired=0,
            )

        seconds_to_window = (window_open_utc - start_utc).total_seconds()
        window_at_mono = start_mono + seconds_to_window
        prearm_at_mono = window_at_mono - self._config.prearm_lead_s
        tight_at_mono = window_at_mono - _TIGHT_LOOP_LEAD_S
        deadline_at_mono = window_at_mono + self._config.global_deadline_s
        not_open_deadline_mono = window_at_mono + self._config.not_open_deadline_s

        prearm_ok = await self._prearm_phase(prearm_at_mono, tight_at_mono)

        await self._sleep_until(tight_at_mono)

        self._log.info("tight_loop_entered")
        while self._clock.monotonic() < window_at_mono:
            await self._clock.sleep(_TIGHT_LOOP_STEP_S)

        fired_at_utc = self._clock.now_utc()
        self._log.info("fire_at", fired_at_utc=fired_at_utc.isoformat())

        return await self._fire_and_retry(
            fired_at_utc=fired_at_utc,
            start_mono=start_mono,
            window_at_mono=window_at_mono,
            not_open_deadline_mono=not_open_deadline_mono,
            deadline_at_mono=deadline_at_mono,
            prearm_ok=prearm_ok,
        )

    # --- phases ---------------------------------------------------------

    async def _prearm_phase(self, prearm_at_mono: float, tight_at_mono: float) -> bool:
        await self._sleep_until(prearm_at_mono)

        # Budget = время от "сейчас" до T−1s минус небольшой guard, чтобы prearm
        # гарантированно завершился до tight loop. Минимум 100мс, иначе смысла нет.
        now_mono = self._clock.monotonic()
        budget = max(_PREARM_MIN_BUDGET_S, tight_at_mono - now_mono - _PREARM_TAIL_GUARD_S)

        self._log.info("prearm_started", budget_s=budget)
        try:
            await asyncio.wait_for(self._client.prearm(), timeout=budget)
        except asyncio.CancelledError:
            raise
        except (TimeoutError, AltegioTransportError, Exception) as e:  # noqa: BLE001
            # Failure here is NOT fatal — мы всё равно попробуем выстрелить.
            # Единичный provisioning handshake стоит дешевле проигранного слота.
            self._log.warning("prearm_failed", cause=type(e).__name__, error=str(e))
            return False

        self._log.info("prearm_done")
        return True

    async def _sleep_until(self, target_mono: float) -> None:
        now = self._clock.monotonic()
        delay = target_mono - now
        if delay > 0:
            await self._clock.sleep(delay)

    def _is_too_close_to_slot(self) -> bool:
        """True если до slot_dt_local осталось меньше min_lead_time_hours.

        Strict less-than: ровно на границе — fire допустим (Altegio даёт refund при
        > 2h, а pretty-close-to-2h всё ещё > по строгому сравнению на стороне CRM).
        Возвращает False, если guard выключен (min_lead_time_hours == 0.0).
        """
        threshold_s = self._config.min_lead_time_hours * 3600.0
        if threshold_s <= 0.0:
            return False
        slot_utc = self._config.slot_dt_local.astimezone(UTC)
        time_to_slot_s = (slot_utc - self._clock.now_utc()).total_seconds()
        return time_to_slot_s < threshold_s

    async def _fire_and_retry(
        self,
        *,
        fired_at_utc: datetime,
        start_mono: float,
        window_at_mono: float,
        not_open_deadline_mono: float,
        deadline_at_mono: float,
        prearm_ok: bool,
    ) -> AttemptResult:
        if self._is_too_close_to_slot():
            self._log.info(
                "result",
                status="error",
                code="too_close_to_slot",
                min_lead_time_hours=self._config.min_lead_time_hours,
            )
            return self._make_result(
                status="error",
                booking=None,
                duplicates=(),
                fired_at_utc=fired_at_utc,
                response_at_utc=None,
                start_mono=start_mono,
                business_code="too_close_to_slot",
                transport_cause=None,
                prearm_ok=prearm_ok,
                shots_fired=0,
            )

        self._active_court_ids = self._select_active_court_ids()
        if self._active_court_ids != self._config.court_ids:
            self._log.info(
                "active_court_subset_selected",
                pool_size=len(self._config.court_ids),
                subset_size=len(self._active_court_ids),
                subset=list(self._active_court_ids),
            )

        pending: set[asyncio.Task[BookingResponse]] = set()
        task_idx: dict[asyncio.Task[BookingResponse], int] = {}
        shots_fired = 0
        response_at_utc: datetime | None = None
        duplicates: list[BookingResponse] = []
        # Parallel to `duplicates`: court_id of each dup so the cancel logger
        # can attribute which court the stranded booking was made on. None
        # entries are tolerated (legacy callers / unmapped indices).
        dup_courts: list[int | None] = []
        # Per-shot transport-retry counter, used for exponential backoff so we
        # don't keep slamming Cloudflare with immediate retries during a CF
        # rate-rule block. Index = same `idx` used everywhere below.
        retry_count_by_idx: dict[int, int] = {}
        # Persist last transport cause across batches so a final no_pending
        # timeout (e.g. all retries skipped near deadline) still carries the
        # diagnostic cause for observability instead of bare None.
        last_transport_cause: str | None = None

        for idx in range(self._config.effective_shots):
            task = self._spawn_shot(idx, deadline_at_mono)
            pending.add(task)
            task_idx[task] = idx
            shots_fired += 1

        try:
            while pending:
                now_mono = self._clock.monotonic()
                remaining = deadline_at_mono - now_mono
                if remaining <= 0:
                    self._log.info("result", status="timeout", reason="global_deadline")
                    return self._make_result(
                        status="timeout",
                        booking=None,
                        duplicates=tuple(duplicates),
                        fired_at_utc=fired_at_utc,
                        response_at_utc=response_at_utc,
                        start_mono=start_mono,
                        business_code=None,
                        transport_cause="global_deadline",
                        prearm_ok=prearm_ok,
                        shots_fired=shots_fired,
                    )

                done, pending = await asyncio.wait(
                    pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    # wait timed out → deadline reached next iteration
                    continue

                # Phase 1: classify every task in `done` without returning from the loop.
                # `done` can contain multiple tasks when responses arrive nearly together;
                # iteration order of a set is non-deterministic, so we must classify all
                # outcomes first and apply priority (win > slot_taken > config_err > ...)
                # afterwards. Returning from inside the loop caused a race where a parallel
                # `slot_taken` could beat an actual `won` response.
                won_booking: BookingResponse | None = None
                won_court_id: int | None = None
                slot_taken_code: str | None = None
                config_err_code: str | None = None
                unknown_code: str | None = None
                unknown_retry_idxs: list[int] = []
                not_open_retry_idxs: list[int] = []
                not_open_code_seen: str | None = None
                # (idx, cause) — cause needed per-shot to pick the right
                # exponential-backoff schedule (cloudflare vs other).
                transport_retry_idx_causes: list[tuple[int, str]] = []
                transport_cause_seen: str | None = None

                for task in done:
                    idx = task_idx.pop(task)
                    if response_at_utc is None:
                        response_at_utc = self._clock.now_utc()

                    if task.cancelled():
                        self._log.info("response_received", idx=idx, status="cancelled")
                        continue

                    exc = task.exception()
                    if exc is None:
                        booking = task.result()
                        self._log.info(
                            "response_received",
                            idx=idx,
                            status="success",
                            record_id=booking.record_id,
                        )
                        shot_court_id = self._active_court_ids[
                            idx % len(self._active_court_ids)
                        ]
                        if won_booking is None:
                            won_booking = booking
                            won_court_id = shot_court_id
                        else:
                            duplicates.append(booking)
                            dup_courts.append(shot_court_id)
                        continue

                    if isinstance(exc, AltegioBusinessError):
                        self._log.info(
                            "response_received",
                            idx=idx,
                            status="business",
                            code=exc.code,
                            http_status=exc.http_status,
                        )
                        if exc.code in SLOT_TAKEN_CODES:
                            if slot_taken_code is None:
                                slot_taken_code = exc.code
                        elif exc.code in CONFIG_ERROR_CODES:
                            if config_err_code is None:
                                config_err_code = exc.code
                        elif exc.code in NOT_OPEN_CODES:
                            not_open_retry_idxs.append(idx)
                            if not_open_code_seen is None:
                                not_open_code_seen = exc.code
                        else:
                            unknown_retry_idxs.append(idx)
                            if unknown_code is None:
                                unknown_code = exc.code
                        continue

                    if isinstance(exc, AltegioTransportError):
                        self._log.info(
                            "response_received",
                            idx=idx,
                            status="transport",
                            cause=exc.cause,
                        )
                        transport_retry_idx_causes.append((idx, exc.cause))
                        if transport_cause_seen is None:
                            transport_cause_seen = exc.cause
                        last_transport_cause = exc.cause
                        continue

                    if isinstance(exc, asyncio.CancelledError):
                        # Self-cancel or external cancel racing with response — ignore.
                        self._log.info("response_received", idx=idx, status="cancelled")
                        continue

                    # Any other exception — unexpected; treat as transport-class retry.
                    self._log.warning(
                        "response_received",
                        idx=idx,
                        status="unknown_exception",
                        exc_type=type(exc).__name__,
                        error=str(exc),
                    )
                    transport_retry_idx_causes.append((idx, type(exc).__name__))
                    if transport_cause_seen is None:
                        transport_cause_seen = type(exc).__name__
                    last_transport_cause = type(exc).__name__

                # Phase 2: apply priority. Win beats all terminal errors (CR blocker).
                if won_booking is not None:
                    await self._drain_for_duplicates(
                        pending, duplicates, dup_courts, task_idx
                    )
                    assert won_court_id is not None  # set together with won_booking
                    await self._persist_win(won_booking, won_court_id, "window")
                    self._log.info(
                        "result",
                        status="won",
                        record_id=won_booking.record_id,
                        duplicates=len(duplicates),
                    )
                    await self._cancel_duplicates(duplicates, dup_courts)
                    await self._notify_win(won_booking, won_court_id, "window")
                    return self._make_result(
                        status="won",
                        booking=won_booking,
                        duplicates=tuple(duplicates),
                        fired_at_utc=fired_at_utc,
                        response_at_utc=response_at_utc,
                        start_mono=start_mono,
                        business_code=None,
                        transport_cause=None,
                        prearm_ok=prearm_ok,
                        shots_fired=shots_fired,
                    )

                if slot_taken_code is not None:
                    await self._cancel_all(pending)
                    self._log.info("result", status="lost", code=slot_taken_code)
                    return self._make_result(
                        status="lost",
                        booking=None,
                        duplicates=tuple(duplicates),
                        fired_at_utc=fired_at_utc,
                        response_at_utc=response_at_utc,
                        start_mono=start_mono,
                        business_code=slot_taken_code,
                        transport_cause=None,
                        prearm_ok=prearm_ok,
                        shots_fired=shots_fired,
                    )

                if config_err_code is not None:
                    await self._cancel_all(pending)
                    self._log.info("result", status="error", code=config_err_code)
                    return self._make_result(
                        status="error",
                        booking=None,
                        duplicates=tuple(duplicates),
                        fired_at_utc=fired_at_utc,
                        response_at_utc=response_at_utc,
                        start_mono=start_mono,
                        business_code=config_err_code,
                        transport_cause=None,
                        prearm_ok=prearm_ok,
                        shots_fired=shots_fired,
                    )

                # Unknown_code fallback ONLY если в этом батче не было ни одного
                # service_not_available. Mix snv + unknown (incident 26.04 02:00 UTC,
                # parser fall-through fix) → silent reclassification: unknowns
                # присоединяются к not_open retry/grace потоку. Иначе один
                # incompletely-parsed shot блокирует grace для целой attempt.
                if unknown_code is not None and not not_open_retry_idxs:
                    await self._cancel_all(pending)
                    self._log.info(
                        "result",
                        status="lost",
                        code=unknown_code,
                        reason="unknown_code_fallback",
                    )
                    return self._make_result(
                        status="lost",
                        booking=None,
                        duplicates=tuple(duplicates),
                        fired_at_utc=fired_at_utc,
                        response_at_utc=response_at_utc,
                        start_mono=start_mono,
                        business_code=unknown_code,
                        transport_cause=None,
                        prearm_ok=prearm_ok,
                        shots_fired=shots_fired,
                    )

                if unknown_retry_idxs and not_open_retry_idxs:
                    # Reclassify: any unknown shot rides on the not_open retry/grace
                    # path together with the snv shots from the same batch.
                    not_open_retry_idxs.extend(unknown_retry_idxs)
                    unknown_retry_idxs = []

                # Not_open + transport retry. If not_open exhausted but transport is still
                # live (global_deadline > not_open_deadline), drop not_open and keep
                # transport retrying until global deadline.
                if not_open_retry_idxs:
                    now_mono = self._clock.monotonic()
                    if now_mono > not_open_deadline_mono:
                        if not transport_retry_idx_causes and not pending:
                            await self._cancel_all(pending)
                            if self._config.grace_polling is not None:
                                grace_deadline_mono = (
                                    window_at_mono + self._config.grace_polling.period_s
                                )
                                return await self._grace_phase(
                                    grace_deadline_at_mono=grace_deadline_mono,
                                    fired_at_utc=fired_at_utc,
                                    start_mono=start_mono,
                                    not_open_code_seen=not_open_code_seen,
                                    duplicates=duplicates,
                                    response_at_utc=response_at_utc,
                                    prearm_ok=prearm_ok,
                                    shots_fired=shots_fired,
                                )
                            self._log.info(
                                "result",
                                status="timeout",
                                reason="not_open_deadline",
                                code=not_open_code_seen,
                            )
                            return self._make_result(
                                status="timeout",
                                booking=None,
                                duplicates=tuple(duplicates),
                                fired_at_utc=fired_at_utc,
                                response_at_utc=response_at_utc,
                                start_mono=start_mono,
                                business_code=not_open_code_seen,
                                transport_cause=None,
                                prearm_ok=prearm_ok,
                                shots_fired=shots_fired,
                            )
                        self._log.info(
                            "not_open_dropped",
                            reason="not_open_deadline_exceeded_but_transport_live",
                            code=not_open_code_seen,
                        )
                        not_open_retry_idxs = []
                    else:
                        await self._clock.sleep(self._config.not_open_retry_ms / 1000.0)

                for idx in not_open_retry_idxs:
                    task = self._spawn_shot(idx, deadline_at_mono)
                    pending.add(task)
                    task_idx[task] = idx
                    shots_fired += 1

                if transport_retry_idx_causes:
                    now_mono = self._clock.monotonic()
                    if now_mono > deadline_at_mono:
                        await self._cancel_all(pending)
                        self._log.info(
                            "result",
                            status="timeout",
                            reason="transport_deadline",
                            cause=transport_cause_seen,
                        )
                        return self._make_result(
                            status="timeout",
                            booking=None,
                            duplicates=tuple(duplicates),
                            fired_at_utc=fired_at_utc,
                            response_at_utc=response_at_utc,
                            start_mono=start_mono,
                            business_code=None,
                            transport_cause=transport_cause_seen,
                            prearm_ok=prearm_ok,
                            shots_fired=shots_fired,
                        )
                    # Exponential backoff per shot: don't slam Cloudflare with
                    # immediate retries during a rate-rule block. Backoff happens
                    # inside each shot's task, so other shots keep racing.
                    for idx, cause in transport_retry_idx_causes:
                        retry_count_by_idx[idx] = retry_count_by_idx.get(idx, 0) + 1
                        retry_count = retry_count_by_idx[idx]
                        if cause == _CLOUDFLARE_TRANSPORT_CAUSE:
                            delay_ms = _exponential_backoff_ms(
                                retry_count,
                                _CF_BACKOFF_INITIAL_MS,
                                _CF_BACKOFF_CAP_MS,
                            )
                        else:
                            delay_ms = _exponential_backoff_ms(
                                retry_count,
                                _OTHER_TRANSPORT_BACKOFF_INITIAL_MS,
                                _OTHER_TRANSPORT_BACKOFF_CAP_MS,
                            )
                        delay_s = delay_ms / 1000.0
                        # Skip retry if the backoff alone would push us past the
                        # global deadline (with a tiny margin to spare). Mark the
                        # shot as a final transport timeout — it doesn't get
                        # re-spawned, so eventually pending drains.
                        time_left_s = deadline_at_mono - now_mono
                        if time_left_s < delay_s + _RETRY_DEADLINE_MIN_MARGIN_S:
                            self._log.info(
                                "transport_retry_skipped",
                                idx=idx,
                                cause=cause,
                                retry_count=retry_count,
                                delay_ms=delay_ms,
                                time_left_s=time_left_s,
                                reason="would_exceed_global_deadline",
                            )
                            continue
                        self._log.info(
                            "transport_retry_scheduled",
                            idx=idx,
                            cause=cause,
                            retry_count=retry_count,
                            delay_ms=delay_ms,
                        )
                        task = self._spawn_shot_with_backoff(
                            idx, deadline_at_mono, delay_s
                        )
                        pending.add(task)
                        task_idx[task] = idx
                        shots_fired += 1

            # pending empty: all shots finished but nothing decisive — treat as timeout.
            # Carry the last-seen transport cause so the operator can tell
            # "we ran out of retry budget under CF block" from "no signal at all".
            self._log.info(
                "result",
                status="timeout",
                reason="no_pending_no_result",
                transport_cause=last_transport_cause,
            )
            return self._make_result(
                status="timeout",
                booking=None,
                duplicates=tuple(duplicates),
                fired_at_utc=fired_at_utc,
                response_at_utc=response_at_utc,
                start_mono=start_mono,
                business_code=None,
                transport_cause=last_transport_cause,
                prearm_ok=prearm_ok,
                shots_fired=shots_fired,
            )
        finally:
            await self._cancel_all(pending)

    # --- helpers --------------------------------------------------------

    def _spawn_shot(self, idx: int, deadline_at_mono: float) -> asyncio.Task[BookingResponse]:
        now = self._clock.monotonic()
        remaining = deadline_at_mono - now
        timeout_s = self._per_shot_timeout(remaining)
        # idx % len(active_court_ids): pool fan-outs одного shot на каждый
        # активный court; legacy (len==1) — все shots дублируются на
        # единственный court. Capped pool: idx ranges 0..effective_shots-1,
        # which equals len(_active_court_ids) — modulo is identity here.
        court_id = self._active_court_ids[idx % len(self._active_court_ids)]
        task = asyncio.create_task(
            self._shot(idx, court_id, timeout_s), name=f"shot-{idx}-court-{court_id}"
        )
        self._log.info("shot_posted", idx=idx, court_id=court_id, timeout_s=timeout_s)
        return task

    def _spawn_shot_with_backoff(
        self,
        idx: int,
        deadline_at_mono: float,
        delay_s: float,
    ) -> asyncio.Task[BookingResponse]:
        """Sleep `delay_s` then fire shot `idx`. Per-shot delay does NOT block
        sibling shots — each retry runs as its own task.
        """
        court_id = self._active_court_ids[idx % len(self._active_court_ids)]

        async def _delayed_shot() -> BookingResponse:
            if delay_s > 0:
                await self._clock.sleep(delay_s)
            now = self._clock.monotonic()
            remaining = deadline_at_mono - now
            timeout_s = self._per_shot_timeout(remaining)
            self._log.info(
                "shot_posted",
                idx=idx,
                court_id=court_id,
                timeout_s=timeout_s,
                backoff_delay_s=delay_s,
            )
            return await self._client.create_booking(
                service_id=self._config.service_id,
                staff_id=court_id,
                slot_dt_local=self._config.slot_dt_local,
                fullname=self._config.fullname,
                phone=self._config.phone,
                email=self._config.email,
                timeout_s=timeout_s,
            )

        return asyncio.create_task(
            _delayed_shot(), name=f"shot-{idx}-court-{court_id}-backoff"
        )

    async def _shot(self, idx: int, court_id: int, timeout_s: float) -> BookingResponse:
        return await self._client.create_booking(
            service_id=self._config.service_id,
            staff_id=court_id,
            slot_dt_local=self._config.slot_dt_local,
            fullname=self._config.fullname,
            phone=self._config.phone,
            email=self._config.email,
            timeout_s=timeout_s,
        )

    def _select_active_court_ids(self) -> tuple[int, ...]:
        """Pick a subset of court_ids to fan out over for this attempt.

        Pool mode (len > 1): if max_parallel_shots is set and < pool size,
        sample WITHOUT replacement to avoid duplicate shots on the same court.
        Otherwise return the full pool (legacy behaviour).

        Single-court (len == 1): always return as-is — parallel_shots controls
        duplication onto the lone court via _spawn_shot's modulo, max_parallel_shots
        is irrelevant.
        """
        all_ids = self._config.court_ids
        cap = self._config.max_parallel_shots
        if cap is None or len(all_ids) <= 1 or cap >= len(all_ids):
            return all_ids
        sampled = self._rng.sample(list(all_ids), cap)
        return tuple(sampled)

    @staticmethod
    def _per_shot_timeout(remaining_to_deadline_s: float) -> float:
        return max(_PER_SHOT_MIN_TIMEOUT_S, remaining_to_deadline_s - _PER_SHOT_TAIL_GUARD_S)

    async def _cancel_all(self, pending: set[asyncio.Task[BookingResponse]]) -> None:
        if not pending:
            return
        for task in pending:
            if not task.done():
                task.cancel()
        # Drain cancellations so background coroutines don't leak past run() return.
        await asyncio.gather(*pending, return_exceptions=True)
        pending.clear()

    async def _persist_win(
        self,
        booking: BookingResponse,
        court_id: int,
        phase: Literal["window", "poll"],
    ) -> None:
        if self._store is None:
            return
        try:
            slot = BookedSlot(
                schema_version=SCHEMA_VERSION,
                record_id=booking.record_id,
                record_hash=booking.record_hash,
                slot_dt_local=self._config.slot_dt_local,
                court_id=court_id,
                service_id=self._config.service_id,
                profile_name=self._config.profile_name,
                phase=phase,
                booked_at_utc=self._clock.now_utc(),
            )
            await self._store.append(slot)
        except Exception:
            # Persistence failure must not bubble into the attempt result —
            # the booking on the server already exists. Log and continue.
            self._log.exception(
                "persistence_append_failed", record_id=booking.record_id
            )

    async def _cancel_duplicates(
        self,
        duplicates: list[BookingResponse],
        dup_courts: list[int | None],
    ) -> None:
        """Best-effort DELETE of stranded duplicate bookings produced by a
        multi-success fan-out.

        Failures are logged with `cancel_response_status_code` and never raised
        — the main `won` outcome must always be returned to the caller. When
        the feature flag is off, only a summary log is emitted with no HTTP
        traffic.
        """
        if not self._cancel_duplicates_enabled:
            if duplicates:
                self._log.info(
                    "duplicates_cancel_skipped",
                    count=len(duplicates),
                    reason="feature_disabled",
                )
            return
        for booking, court in zip(duplicates, dup_courts, strict=False):
            try:
                await self._client.cancel_booking(
                    booking.record_id,
                    booking.record_hash,
                    timeout_s=5.0,
                )
                self._log.info(
                    "duplicate_cancelled",
                    record_id=booking.record_id,
                    court_id=court,
                    cancel_response_status_code=200,
                )
            except AltegioBusinessError as exc:
                self._log.warning(
                    "duplicate_cancel_business_error",
                    record_id=booking.record_id,
                    court_id=court,
                    code=exc.code,
                    message=exc.message,
                    http_status=exc.http_status,
                    cancel_response_status_code=exc.http_status,
                )
            except AltegioTransportError as exc:
                self._log.warning(
                    "duplicate_cancel_transport_error",
                    record_id=booking.record_id,
                    court_id=court,
                    cause=exc.cause,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — never crash main path on cancel
                self._log.warning(
                    "duplicate_cancel_network_error",
                    record_id=booking.record_id,
                    court_id=court,
                    exc_type=type(exc).__name__,
                    error=str(exc),
                )

    async def _drain_for_duplicates(
        self,
        pending: set[asyncio.Task[BookingResponse]],
        duplicates: list[BookingResponse],
        dup_courts: list[int | None],
        task_idx: dict[asyncio.Task[BookingResponse], int],
    ) -> None:
        """После win — cancel остальные, но собрать уже-готовые success как duplicates.

        Каждому собранному дубликату ставим в соответствие court_id через
        task_idx → _active_court_ids, чтобы cancel-логика могла залогировать
        корт. Несопоставленные задачи (теоретически невозможно, но defensive)
        получают None.
        """
        if not pending:
            return
        # Freeze iteration order: `pending` is a set, so we must materialise it before
        # awaiting gather and zipping results, otherwise task/result correspondence is
        # not guaranteed.
        tasks_list = list(pending)
        for task in tasks_list:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks_list, return_exceptions=True)
        for task, res in zip(tasks_list, results, strict=True):
            if task.cancelled():
                continue
            if isinstance(res, BookingResponse):
                duplicates.append(res)
                idx = task_idx.get(task)
                if idx is not None and self._active_court_ids:
                    dup_courts.append(
                        self._active_court_ids[idx % len(self._active_court_ids)]
                    )
                else:
                    dup_courts.append(None)
        pending.clear()

    # --- grace mode -----------------------------------------------------

    async def _grace_phase(
        self,
        *,
        grace_deadline_at_mono: float,
        fired_at_utc: datetime,
        start_mono: float,
        not_open_code_seen: str | None,
        duplicates: list[BookingResponse],
        response_at_utc: datetime | None,
        prearm_ok: bool,
        shots_fired: int,
    ) -> AttemptResult:
        """Polls search/timeslots каждые grace_polling.interval_s до grace_deadline.

        На первый bookable matched slot — fan-out create_booking на ВСЕ court_ids.
        Win → return won (phase="window"). Config error → return error.
        Lost / transport-only / все snv → продолжаем polling.
        """
        grace = self._config.grace_polling
        assert grace is not None  # invariant: caller already checked
        self._log.info(
            "grace_started",
            grace_period_s=grace.period_s,
            grace_interval_s=grace.interval_s,
            not_open_code_seen=not_open_code_seen,
        )

        target_almaty = (
            self._config.slot_dt_local
            if self._config.slot_dt_local.tzinfo is ALMATY
            else self._config.slot_dt_local.astimezone(ALMATY)
        )
        target_date = target_almaty.date()
        staff_ids = list(self._config.court_ids)

        local_shots_fired = shots_fired
        local_response_at_utc = response_at_utc

        while True:
            await self._clock.sleep(grace.interval_s)

            now_mono = self._clock.monotonic()
            if now_mono > grace_deadline_at_mono:
                self._log.info("grace_period_exhausted")
                return self._make_result(
                    status="timeout",
                    booking=None,
                    duplicates=tuple(duplicates),
                    fired_at_utc=fired_at_utc,
                    response_at_utc=local_response_at_utc,
                    start_mono=start_mono,
                    business_code="grace_period_exhausted",
                    transport_cause=None,
                    prearm_ok=prearm_ok,
                    shots_fired=local_shots_fired,
                )

            try:
                slots = await self._client.search_timeslots(
                    date_local=target_date,
                    staff_ids=staff_ids,
                    timeout_s=_GRACE_SEARCH_TIMEOUT_S,
                )
            except AltegioBusinessError as e:
                if e.code in CONFIG_ERROR_CODES:
                    self._log.error(
                        "grace_search_config_err",
                        code=e.code,
                        http_status=e.http_status,
                    )
                    return self._make_result(
                        status="error",
                        booking=None,
                        duplicates=tuple(duplicates),
                        fired_at_utc=fired_at_utc,
                        response_at_utc=local_response_at_utc,
                        start_mono=start_mono,
                        business_code=e.code,
                        transport_cause=None,
                        prearm_ok=prearm_ok,
                        shots_fired=local_shots_fired,
                    )
                self._log.warning(
                    "grace_search_business_err",
                    code=e.code,
                    http_status=e.http_status,
                )
                continue
            except AltegioTransportError as e:
                self._log.warning("grace_search_transport_err", cause=e.cause)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — never let grace crash on unexpected
                self._log.warning(
                    "grace_search_unknown_err",
                    exc_type=type(e).__name__,
                    error=str(e),
                )
                continue

            if not self._has_bookable(slots, target_almaty):
                self._log.info("grace_search_no_match", slot_count=len(slots))
                continue

            self._log.info("grace_search_match_fire")
            fire_result = await self._fire_shots_grace(
                fired_at_utc=fired_at_utc,
                start_mono=start_mono,
                duplicates=duplicates,
                prearm_ok=prearm_ok,
                shots_fired_so_far=local_shots_fired,
            )
            local_shots_fired = fire_result.shots_fired
            if fire_result.response_at_utc is not None:
                local_response_at_utc = fire_result.response_at_utc

            if fire_result.status == "won":
                return fire_result
            if fire_result.status == "error":
                return fire_result
            # status == "lost" with all-snv OR transport-only: continue polling.
            continue

    def _has_bookable(self, slots: list[TimeSlot], target_almaty: datetime) -> bool:
        """True если хотя бы один slot матчит наш slot_dt_local и is_bookable."""
        for slot in slots:
            if not slot.is_bookable:
                continue
            if slot.dt.astimezone(ALMATY) != target_almaty:
                continue
            if slot.staff_id is not None and slot.staff_id not in self._config.court_ids:
                continue
            return True
        return False

    async def _fire_shots_grace(
        self,
        *,
        fired_at_utc: datetime,
        start_mono: float,
        duplicates: list[BookingResponse],
        prearm_ok: bool,
        shots_fired_so_far: int,
    ) -> AttemptResult:
        """Fan-out create_booking на ВСЕ court_ids с per-shot timeout 5s.

        Возвращает AttemptResult с status:
          - "won" — хотя бы один shot succeeded
          - "error" — config error (unauthorized) → caller exits
          - "lost" — другие исходы (all snv / transport-only / unknown) → caller continues polling
        """
        if self._is_too_close_to_slot():
            self._log.info(
                "grace_result",
                status="error",
                code="too_close_to_slot",
                min_lead_time_hours=self._config.min_lead_time_hours,
            )
            return self._make_result(
                status="error",
                booking=None,
                duplicates=tuple(duplicates),
                fired_at_utc=fired_at_utc,
                response_at_utc=None,
                start_mono=start_mono,
                business_code="too_close_to_slot",
                transport_cause=None,
                prearm_ok=prearm_ok,
                shots_fired=shots_fired_so_far,
            )

        pending: set[asyncio.Task[BookingResponse]] = set()
        task_idx: dict[asyncio.Task[BookingResponse], int] = {}
        local_shots_fired = shots_fired_so_far
        local_response_at_utc: datetime | None = None
        # Grace-mode dups appended to caller's `duplicates` list — track courts
        # in a parallel local list so we can cancel them after the grace win.
        grace_dup_courts: list[int | None] = []

        for idx, court_id in enumerate(self._config.court_ids):
            task = asyncio.create_task(
                self._shot(idx, court_id, _GRACE_PER_SHOT_TIMEOUT_S),
                name=f"grace-shot-{idx}-court-{court_id}",
            )
            pending.add(task)
            task_idx[task] = idx
            local_shots_fired += 1

        try:
            won_booking: BookingResponse | None = None
            won_court_id: int | None = None
            config_err_code: str | None = None
            transport_cause_seen: str | None = None
            unknown_code: str | None = None
            slot_taken_code: str | None = None
            not_open_seen: bool = False

            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    idx = task_idx.pop(task)
                    if local_response_at_utc is None:
                        local_response_at_utc = self._clock.now_utc()

                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is None:
                        booking = task.result()
                        self._log.info(
                            "grace_response",
                            idx=idx,
                            status="success",
                            record_id=booking.record_id,
                        )
                        if won_booking is None:
                            won_booking = booking
                            won_court_id = self._config.court_ids[idx]
                        else:
                            duplicates.append(booking)
                            grace_dup_courts.append(self._config.court_ids[idx])
                        continue

                    if isinstance(exc, AltegioBusinessError):
                        self._log.info(
                            "grace_response",
                            idx=idx,
                            status="business",
                            code=exc.code,
                            http_status=exc.http_status,
                        )
                        if exc.code in CONFIG_ERROR_CODES:
                            if config_err_code is None:
                                config_err_code = exc.code
                        elif exc.code in NOT_OPEN_CODES:
                            not_open_seen = True
                        elif exc.code in SLOT_TAKEN_CODES:
                            if slot_taken_code is None:
                                slot_taken_code = exc.code
                        else:
                            if unknown_code is None:
                                unknown_code = exc.code
                        continue

                    if isinstance(exc, AltegioTransportError):
                        self._log.info(
                            "grace_response",
                            idx=idx,
                            status="transport",
                            cause=exc.cause,
                        )
                        if transport_cause_seen is None:
                            transport_cause_seen = exc.cause
                        continue

                    if isinstance(exc, asyncio.CancelledError):
                        continue

                    self._log.warning(
                        "grace_response",
                        idx=idx,
                        status="unknown_exception",
                        exc_type=type(exc).__name__,
                        error=str(exc),
                    )
                    if transport_cause_seen is None:
                        transport_cause_seen = type(exc).__name__

            if won_booking is not None:
                assert won_court_id is not None
                await self._persist_win(won_booking, won_court_id, "window")
                self._log.info(
                    "grace_result",
                    status="won",
                    record_id=won_booking.record_id,
                    duplicates=len(duplicates),
                )
                # Cancel the dups produced *in this grace fire*. We pass only
                # the locally-tracked subset (grace_dup_courts) — earlier dups
                # from the original window fire have already been processed
                # before grace started.
                grace_dups_only = duplicates[len(duplicates) - len(grace_dup_courts):]
                await self._cancel_duplicates(grace_dups_only, grace_dup_courts)
                await self._notify_win(won_booking, won_court_id, "window")
                return self._make_result(
                    status="won",
                    booking=won_booking,
                    duplicates=tuple(duplicates),
                    fired_at_utc=fired_at_utc,
                    response_at_utc=local_response_at_utc,
                    start_mono=start_mono,
                    business_code=None,
                    transport_cause=None,
                    prearm_ok=prearm_ok,
                    shots_fired=local_shots_fired,
                )

            if config_err_code is not None:
                self._log.error("grace_result", status="error", code=config_err_code)
                return self._make_result(
                    status="error",
                    booking=None,
                    duplicates=tuple(duplicates),
                    fired_at_utc=fired_at_utc,
                    response_at_utc=local_response_at_utc,
                    start_mono=start_mono,
                    business_code=config_err_code,
                    transport_cause=None,
                    prearm_ok=prearm_ok,
                    shots_fired=local_shots_fired,
                )

            # Anything else (slot_taken / unknown / not_open / transport-only): tell caller
            # to keep polling. business_code preserved for observability; status="lost" so
            # outer grace loop continues.
            preserved_code = slot_taken_code or unknown_code
            if preserved_code is None and not_open_seen:
                preserved_code = "service_not_available"
            self._log.info(
                "grace_result",
                status="lost",
                business_code=preserved_code,
                transport_cause=transport_cause_seen,
            )
            return self._make_result(
                status="lost",
                booking=None,
                duplicates=tuple(duplicates),
                fired_at_utc=fired_at_utc,
                response_at_utc=local_response_at_utc,
                start_mono=start_mono,
                business_code=preserved_code,
                transport_cause=transport_cause_seen,
                prearm_ok=prearm_ok,
                shots_fired=local_shots_fired,
            )
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    def _make_result(
        self,
        *,
        status: AttemptStatus,
        booking: BookingResponse | None,
        duplicates: tuple[BookingResponse, ...],
        fired_at_utc: datetime | None,
        response_at_utc: datetime | None,
        start_mono: float,
        business_code: str | None,
        transport_cause: str | None,
        prearm_ok: bool,
        shots_fired: int,
    ) -> AttemptResult:
        duration_ms = (self._clock.monotonic() - start_mono) * 1000.0
        result = AttemptResult(
            status=status,
            booking=booking,
            duplicates=duplicates,
            fired_at_utc=fired_at_utc,
            response_at_utc=response_at_utc,
            duration_ms=duration_ms,
            business_code=business_code,
            transport_cause=transport_cause,
            prearm_ok=prearm_ok,
            shots_fired=shots_fired,
            attempt_id=self._attempt_id,
            phase="window",
        )
        self._schedule_terminal_notification(result)
        return result

    async def _notify_win(
        self,
        booking: BookingResponse,
        court_id: int,
        phase: Literal["window", "poll"],
    ) -> None:
        if not self._notifier.is_active:
            return
        text = format_win_message(
            slot_dt_local=self._config.slot_dt_local,
            profile_name=self._config.profile_name,
            pool_key=self._config.pool_key,
            booking=booking,
            court_id=court_id,
            phase=phase,
        )
        # Awaited synchronously: `send()` is best-effort and never raises, so
        # blocking here is bounded by the per-request 5s timeout. Doing this
        # synchronously avoids an orphan task when the engine returns into a
        # parent that completes immediately.
        await self._notifier.send(text)

    def _schedule_terminal_notification(self, result: AttemptResult) -> None:
        """Fire a Telegram message for timeout / lost terminal results.

        Win is handled separately in `_notify_win` (needs court_id). Error and
        `won_by_sibling` lost (cross-profile dedup) are intentionally silent.
        Dispatch is fire-and-forget so `_make_result` stays synchronous.
        """
        if not self._notifier.is_active:
            return
        if result.status == "timeout":
            text = format_timeout_message(
                slot_dt_local=self._config.slot_dt_local,
                profile_name=self._config.profile_name,
                pool_key=self._config.pool_key,
                phase=result.phase or "window",
                duration_ms=result.duration_ms,
                shots_fired=result.shots_fired,
            )
        elif result.status == "lost" and result.business_code != "won_by_sibling":
            text = format_lost_message(
                slot_dt_local=self._config.slot_dt_local,
                profile_name=self._config.profile_name,
                pool_key=self._config.pool_key,
                business_code=result.business_code,
                phase=result.phase or "window",
            )
        else:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Called outside an event loop (defensive — should not happen in
            # production, but unit-test callers may construct results synchronously).
            return
        task = asyncio.create_task(self._notifier.send(text))
        self._notification_tasks.add(task)
        task.add_done_callback(self._notification_tasks.discard)
