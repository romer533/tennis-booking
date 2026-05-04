from __future__ import annotations

import asyncio
import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

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
from tennis_booking.persistence import BookedSlot, BookingStore
from tennis_booking.persistence.models import SCHEMA_VERSION

from .attempt import AttemptConfig, AttemptResult, AttemptStatus
from .codes import CONFIG_ERROR_CODES, SLOT_TAKEN_CODES
from .poll_cache import PollResultCache

__all__ = ["PollAttempt", "PollConfigData"]

_logger = structlog.get_logger(__name__)

_PER_SHOT_TIMEOUT_S = 5.0
_SEARCH_TIMEOUT_S = 5.0

# Jitter envelope around interval_s to break synchronised polling waves.
# Initial jitter: U(0, _INITIAL_JITTER_FRAC * interval_s) one-shot before the
# first tick. Per-tick jitter: ±_TICK_JITTER_FRAC * interval_s on every sleep.
_INITIAL_JITTER_FRAC = 0.5
_TICK_JITTER_FRAC = 0.1
# SystemRandom is process-instance random — much higher quality than the
# default Mersenne twister for spreading concurrent polls. Cost is negligible
# compared to the network round-trip we are spreading.
_jitter_rng = random.SystemRandom()


def _synthesize_pool_key(court_ids: tuple[int, ...]) -> str:
    """Deterministic fallback pool key when caller doesn't provide one.

    Sorted to canonicalise (cf. tuple identity vs set equality). Two polls
    with the same staff_ids set must collide; two with different sets must
    not. Format chosen to be obviously synthetic in logs ("courts:5,6,7" vs
    a real pool name).
    """
    return "courts:" + ",".join(str(c) for c in sorted(court_ids))


@dataclass(frozen=True)
class PollConfigData:
    """Параметры poll режима — unpacked из PollConfig schema.

    Engine не зависит от pydantic-слоя, поэтому принимаем чистые поля.
    """

    interval_s: int
    start_offset_days: int

    def __post_init__(self) -> None:
        if self.interval_s < 10:
            raise ValueError(f"interval_s must be >= 10, got {self.interval_s}")
        if self.start_offset_days < 1:
            raise ValueError(
                f"start_offset_days must be >= 1, got {self.start_offset_days}"
            )
        if self.start_offset_days > 30:
            raise ValueError(
                f"start_offset_days must be <= 30, got {self.start_offset_days}"
            )


class PollAttempt:
    """Monitor mode: polls search/timeslots, fires create_booking on is_bookable.

    Two modes (selected via `post_window_mode`):

    Pre-window mode (default, post_window_mode=False):
      Effective start = max(now, slot_dt_local_utc - start_offset_days);
      stop_at = slot_dt_local_utc. Used to catch slots opening early before
      the natural T-2d 07:00 window — runs alongside a window task.

    Post-window mode (post_window_mode=True):
      Effective start = now (immediate); stop_at = slot_dt_local_utc - min_lead.
      Used after a window task has resolved with status=lost/timeout to keep
      hunting for cancellations released by other users (typical 24-48h
      before the slot). Stops min_lead_time_hours before the slot itself,
      so the existing fire-time guard never has to refuse a fire.

    State machine (both modes):
      1. sleep until effective_start (no-op in post_window_mode)
      2. while now_utc < stop_at_utc:
         a. if won_event.is_set() → early return (cancelled)
         b. POST /booking/search/timeslots/ (date=slot_local_date, staff_ids=court_ids)
         c. find matching slot (dt == slot_dt_local AND is_bookable=True)
         d. if found:
            - won_event.set() to claim fire exclusivity
            - fan-out create_booking across court_ids
            - on success: return won
            - on lost/transport: clear won_event, continue polling
            - on config_err (unauthorized): return error immediately
         e. sleep interval_s (respect cancellation)
      3. now_utc >= stop_at_utc → timeout (reason depends on mode).
    """

    def __init__(
        self,
        attempt_config: AttemptConfig,
        poll: PollConfigData,
        client: AltegioClient,
        clock: Clock,
        won_event: asyncio.Event | None = None,
        store: BookingStore | None = None,
        *,
        post_window_mode: bool = False,
        cache: PollResultCache | None = None,
        pool_key: str | None = None,
        rng: random.Random | None = None,
        cancel_duplicates_enabled: bool = True,
    ) -> None:
        self._config = attempt_config
        self._poll = poll
        self._client = client
        self._clock = clock
        self._store = store
        self._won_event = won_event if won_event is not None else asyncio.Event()
        self._post_window_mode = post_window_mode
        self._cache = cache
        self._cancel_duplicates_enabled = cancel_duplicates_enabled
        # When pool_key is not provided, fall back to a deterministic synthetic
        # key derived from the full court_ids tuple. This guarantees that polls
        # which differ ONLY by profile (same date, same staff_ids) coalesce,
        # while polls with different court sets stay separated (correctness:
        # different court sets fetch different Altegio responses).
        self._pool_key = pool_key if pool_key is not None else _synthesize_pool_key(
            attempt_config.court_ids
        )
        # SystemRandom for production (CSPRNG, no seedable state); injectable
        # for reproducible tests of subset selection. Reused across every fire
        # within this attempt — but a fresh subset is picked per fire (see
        # _select_active_court_ids) to spread load on successive cancellations.
        # TODO(refactor): _select_active_court_ids is duplicated with
        # BookingAttempt._select_active_court_ids. Extract to engine/_subset.py
        # if a third caller appears.
        self._rng: random.Random = rng if rng is not None else random.SystemRandom()
        self._used = False
        self._attempt_id = uuid.uuid4().hex
        log_bindings: dict[str, object] = {
            "attempt_id": self._attempt_id,
            "slot_dt_local": attempt_config.slot_dt_local.isoformat(),
            "dry_run": client.config.dry_run,
            "phase": "post_window_poll" if post_window_mode else "poll",
            "poll_interval_s": poll.interval_s,
            "poll_start_offset_days": poll.start_offset_days,
            "pool_key": self._pool_key,
        }
        if len(attempt_config.court_ids) <= 7:
            log_bindings["court_ids"] = tuple(attempt_config.court_ids)
        else:
            log_bindings["court_id_primary"] = attempt_config.court_ids[0]
            log_bindings["court_count"] = len(attempt_config.court_ids)
        self._log = _logger.bind(**log_bindings)

    async def run(self) -> AttemptResult:
        if self._used:
            raise RuntimeError("PollAttempt.run() is single-shot; create a new instance")
        self._used = True

        start_utc = self._clock.now_utc()
        start_mono = self._clock.monotonic()

        slot_utc = self._config.slot_dt_local.astimezone(start_utc.tzinfo)

        if self._post_window_mode:
            # Post-window: start polling immediately; stop min_lead_time_hours
            # before the slot so the existing fire-time guard inside _fire_shots
            # never has to refuse a fire (it would be wasted work).
            effective_start_utc = start_utc
            min_lead_s = self._config.min_lead_time_hours * 3600.0
            stop_at_utc = slot_utc - timedelta(seconds=min_lead_s)
            stopped_reason = "post_window_window_closed"
        else:
            effective_start_utc = slot_utc - timedelta(days=self._poll.start_offset_days)
            if effective_start_utc < start_utc:
                effective_start_utc = start_utc
            stop_at_utc = slot_utc
            stopped_reason = "slot_passed"

        self._log.info(
            "poll_scheduled",
            effective_start_utc=effective_start_utc.isoformat(),
            stop_at_utc=stop_at_utc.isoformat(),
        )

        if stop_at_utc <= start_utc:
            self._log.warning(
                "slot_passed_before_start",
                now_utc=start_utc.isoformat(),
                stop_at_utc=stop_at_utc.isoformat(),
            )
            return self._make_result(
                status="timeout",
                booking=None,
                duplicates=(),
                fired_at_utc=None,
                response_at_utc=None,
                start_mono=start_mono,
                business_code=None,
                transport_cause=stopped_reason,
                shots_fired=0,
            )

        await self._sleep_until_utc(effective_start_utc)

        # Initial jitter: spread the first tick across [0, interval/2). Without
        # it, when SchedulerLoop spawns N polls in a tight `_spawn_attempts`
        # loop, all of them call `search_timeslots` within ms of each other
        # every interval_s — the synchronised pattern Cloudflare flags as a bot.
        # Cache absorbs this for same-(date,pool) polls; jitter handles
        # different (date,pool) polls.
        await self._initial_jitter_sleep()

        while True:
            if self._won_event.is_set():
                self._log.info("poll_cancelled_by_sibling")
                return self._make_result(
                    status="lost",
                    booking=None,
                    duplicates=(),
                    fired_at_utc=None,
                    response_at_utc=None,
                    start_mono=start_mono,
                    business_code="won_by_sibling",
                    transport_cause=None,
                    shots_fired=0,
                )

            now_utc = self._clock.now_utc()
            if now_utc >= stop_at_utc:
                self._log.info("poll_stopped", reason=stopped_reason)
                return self._make_result(
                    status="timeout",
                    booking=None,
                    duplicates=(),
                    fired_at_utc=None,
                    response_at_utc=None,
                    start_mono=start_mono,
                    business_code=None,
                    transport_cause=stopped_reason,
                    shots_fired=0,
                )

            try:
                slots = await self._search_timeslots()
            except AltegioBusinessError as e:
                if e.code in CONFIG_ERROR_CODES:
                    self._log.error(
                        "poll_search_config_err",
                        code=e.code,
                        http_status=e.http_status,
                    )
                    return self._make_result(
                        status="error",
                        booking=None,
                        duplicates=(),
                        fired_at_utc=None,
                        response_at_utc=None,
                        start_mono=start_mono,
                        business_code=e.code,
                        transport_cause=None,
                        shots_fired=0,
                    )
                self._log.warning(
                    "poll_search_business_err",
                    code=e.code,
                    http_status=e.http_status,
                )
                await self._sleep_interval()
                continue
            except AltegioTransportError as e:
                self._log.warning("poll_search_transport_err", cause=e.cause)
                await self._sleep_interval()
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — never let poll crash on unexpected
                self._log.warning(
                    "poll_search_unknown_err",
                    exc_type=type(e).__name__,
                    error=str(e),
                )
                await self._sleep_interval()
                continue

            bookable = self._find_bookable(slots)
            if bookable is None:
                self._log.info("poll_tick", bookable=False, slot_count=len(slots))
                await self._sleep_interval()
                continue

            self._log.info("poll_detected_bookable", matched_count=len(bookable))

            # Claim fire exclusivity before POST. If we lose with an explicit
            # business code (slot_taken / unknown_code fallback), clear so that
            # the window-sibling (if still live) can still fire.
            self._won_event.set()

            result = await self._fire_shots(start_mono=start_mono)
            if result.status == "won":
                return result

            if result.status == "error":
                return result

            # Only clear on business-class loss with no transport uncertainty.
            # Transport / timeout loss → request reached server, response lost:
            # booking may have actually been created. Leaving the event set
            # prevents the window-sibling from firing a duplicate POST.
            if (
                result.status == "lost"
                and result.business_code is not None
                and result.transport_cause is None
            ):
                self._won_event.clear()
                self._log.info(
                    "poll_fire_miss_continuing",
                    status=result.status,
                    business_code=result.business_code,
                    transport_cause=result.transport_cause,
                )
            else:
                self._log.info(
                    "poll_fire_uncertain_keeping_claim",
                    status=result.status,
                    business_code=result.business_code,
                    transport_cause=result.transport_cause,
                )
            await self._sleep_interval()

    async def _sleep_interval(self) -> None:
        # Per-tick jitter ±10% — even after the cache collapses N→1, the polls
        # for *different* (date, pool) combinations still happen on identical
        # tick boundaries if not jittered. This shaves the remaining sync.
        base = float(self._poll.interval_s)
        jitter = _jitter_rng.uniform(-base * _TICK_JITTER_FRAC, base * _TICK_JITTER_FRAC)
        delay = base + jitter
        if delay <= 0:
            # Defensive: should be impossible with _TICK_JITTER_FRAC < 1.0,
            # but if someone bumps the constant, never go negative.
            delay = base
        await self._clock.sleep(delay)

    async def _initial_jitter_sleep(self) -> None:
        # One-shot smear before the first tick so that N polls spawned in the
        # same SchedulerLoop pass don't fire their first search_timeslots in
        # a 30 ms cluster. Range [0, interval/2) is wide enough to break any
        # observable pattern while still letting the first poll fetch quickly
        # in the worst case (no fresh cache entry).
        delay = _jitter_rng.uniform(0.0, self._poll.interval_s * _INITIAL_JITTER_FRAC)
        if delay <= 0:
            return
        await self._clock.sleep(delay)

    async def _search_timeslots(self) -> list[TimeSlot]:
        # Compute date IN ALMATY: slot_dt_local is already Almaty-tz-aware,
        # but using `.date()` directly is brittle if the tzinfo is something
        # else (defensive — AttemptConfig validates ALMATY, but we call
        # .astimezone explicitly to make the timezone semantics visible at
        # the cache boundary).
        date_local = self._config.slot_dt_local.astimezone(ALMATY).date()

        async def _fetch() -> list[TimeSlot]:
            return await self._client.search_timeslots(
                date_local=date_local,
                staff_ids=list(self._config.court_ids),
                timeout_s=_SEARCH_TIMEOUT_S,
            )

        if self._cache is None:
            return await _fetch()
        return await self._cache.get_or_fetch(date_local, self._pool_key, _fetch)

    async def _sleep_until_utc(self, target_utc: datetime) -> None:
        now_utc = self._clock.now_utc()
        delay = (target_utc - now_utc).total_seconds()
        if delay > 0:
            await self._clock.sleep(delay)

    def _find_bookable(self, slots: list[TimeSlot]) -> list[int] | None:
        """Return list of court_ids for which an is_bookable timeslot at slot_dt_local
        exists. None if no match at all (tick miss).

        Matching strategy:
          - prefer per-court match if slot.staff_id is set (API may return multiple
            slots, one per staff).
          - if slot.staff_id is None → we assume filter by staff_ids in request was
            applied server-side; any bookable slot at slot_dt_local counts for all
            configured court_ids.
        """
        target = self._config.slot_dt_local
        # Normalize to Almaty in case target uses a different tz (shouldn't happen
        # — AttemptConfig validates ALMATY — but defensive).
        target_almaty = target.astimezone(ALMATY) if target.tzinfo is not ALMATY else target

        per_court_bookable: set[int] = set()
        any_bookable = False
        for slot in slots:
            if not slot.is_bookable:
                continue
            if slot.dt.astimezone(ALMATY) != target_almaty:
                continue
            any_bookable = True
            if slot.staff_id is not None and slot.staff_id in self._config.court_ids:
                per_court_bookable.add(slot.staff_id)

        if per_court_bookable:
            return [cid for cid in self._config.court_ids if cid in per_court_bookable]
        if any_bookable:
            return list(self._config.court_ids)
        return None

    def _select_active_court_ids(self) -> tuple[int, ...]:
        """Pick a subset of court_ids to fan out over for this fire.

        Pool mode (len > 1): if max_parallel_shots is set and < pool size,
        sample WITHOUT replacement to avoid duplicate POSTs on the same court
        and to cap the per-fire request count (Cloudflare rate-rule defense
        — see docs/incidents/2026-04-30-poll-fanout-duplicate.md context).
        Otherwise return the full pool (legacy behaviour, backward compat).

        Single-court (len == 1): always return as-is — there is nothing to
        sample down to and parallel_shots semantics from BookingAttempt do
        not apply to poll fire (poll always fires exactly one shot per court).
        """
        all_ids = self._config.court_ids
        cap = self._config.max_parallel_shots
        if cap is None or len(all_ids) <= 1 or cap >= len(all_ids):
            return all_ids
        sampled = self._rng.sample(list(all_ids), cap)
        return tuple(sampled)

    def _is_too_close_to_slot(self) -> bool:
        """True если до slot_dt_local осталось меньше min_lead_time_hours.

        Strict less-than: ровно на границе fire допустим. См. attempt.py docstring.
        """
        threshold_s = self._config.min_lead_time_hours * 3600.0
        if threshold_s <= 0.0:
            return False
        slot_utc = self._config.slot_dt_local.astimezone(UTC)
        time_to_slot_s = (slot_utc - self._clock.now_utc()).total_seconds()
        return time_to_slot_s < threshold_s

    async def _fire_shots(self, *, start_mono: float) -> AttemptResult:
        if self._is_too_close_to_slot():
            self._log.info(
                "poll_result",
                status="error",
                code="too_close_to_slot",
                min_lead_time_hours=self._config.min_lead_time_hours,
            )
            return self._make_result(
                status="error",
                booking=None,
                duplicates=(),
                fired_at_utc=None,
                response_at_utc=None,
                start_mono=start_mono,
                business_code="too_close_to_slot",
                transport_cause=None,
                shots_fired=0,
            )

        fired_at_utc = self._clock.now_utc()
        self._log.info("poll_fire_at", fired_at_utc=fired_at_utc.isoformat())

        active_court_ids = self._select_active_court_ids()
        if active_court_ids != self._config.court_ids:
            self._log.info(
                "poll_active_court_subset_selected",
                pool_size=len(self._config.court_ids),
                subset_size=len(active_court_ids),
                subset=list(active_court_ids),
            )

        pending: set[asyncio.Task[BookingResponse]] = set()
        task_idx: dict[asyncio.Task[BookingResponse], int] = {}
        shots_fired = 0
        response_at_utc = None
        duplicates: list[BookingResponse] = []
        # Parallel to `duplicates`: court_id of each dup so cancel_logger can
        # attribute which court the stranded booking was made on.
        dup_courts: list[int | None] = []

        for idx, court_id in enumerate(active_court_ids):
            task = asyncio.create_task(
                self._client.create_booking(
                    service_id=self._config.service_id,
                    staff_id=court_id,
                    slot_dt_local=self._config.slot_dt_local,
                    fullname=self._config.fullname,
                    phone=self._config.phone,
                    email=self._config.email,
                    timeout_s=_PER_SHOT_TIMEOUT_S,
                ),
                name=f"poll-shot-{idx}-court-{court_id}",
            )
            pending.add(task)
            task_idx[task] = idx
            shots_fired += 1
            self._log.info("poll_shot_posted", idx=idx, court_id=court_id)

        try:
            won_booking: BookingResponse | None = None
            won_court_id: int | None = None
            slot_taken_code: str | None = None
            config_err_code: str | None = None
            transport_cause_seen: str | None = None
            unknown_code: str | None = None

            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    idx = task_idx.pop(task)
                    if response_at_utc is None:
                        response_at_utc = self._clock.now_utc()
                    if task.cancelled():
                        continue
                    exc = task.exception()
                    if exc is None:
                        booking = task.result()
                        self._log.info(
                            "poll_response",
                            idx=idx,
                            status="success",
                            record_id=booking.record_id,
                        )
                        if won_booking is None:
                            won_booking = booking
                            won_court_id = active_court_ids[idx]
                        else:
                            duplicates.append(booking)
                            dup_courts.append(active_court_ids[idx])
                        continue
                    if isinstance(exc, AltegioBusinessError):
                        self._log.info(
                            "poll_response",
                            idx=idx,
                            status="business",
                            code=exc.code,
                            http_status=exc.http_status,
                        )
                        if exc.code in CONFIG_ERROR_CODES:
                            if config_err_code is None:
                                config_err_code = exc.code
                        elif exc.code in SLOT_TAKEN_CODES:
                            if slot_taken_code is None:
                                slot_taken_code = exc.code
                        else:
                            if unknown_code is None:
                                unknown_code = exc.code
                        continue
                    if isinstance(exc, AltegioTransportError):
                        self._log.info(
                            "poll_response",
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
                        "poll_response",
                        idx=idx,
                        status="unknown_exception",
                        exc_type=type(exc).__name__,
                        error=str(exc),
                    )
                    if transport_cause_seen is None:
                        transport_cause_seen = type(exc).__name__

            if won_booking is not None:
                assert won_court_id is not None  # set together with won_booking
                await self._persist_win(won_booking, won_court_id)
                self._log.info(
                    "poll_result",
                    status="won",
                    record_id=won_booking.record_id,
                    duplicates=len(duplicates),
                )
                await self._cancel_duplicates(duplicates, dup_courts)
                return self._make_result(
                    status="won",
                    booking=won_booking,
                    duplicates=tuple(duplicates),
                    fired_at_utc=fired_at_utc,
                    response_at_utc=response_at_utc,
                    start_mono=start_mono,
                    business_code=None,
                    transport_cause=None,
                    shots_fired=shots_fired,
                )

            if config_err_code is not None:
                self._log.error("poll_result", status="error", code=config_err_code)
                return self._make_result(
                    status="error",
                    booking=None,
                    duplicates=tuple(duplicates),
                    fired_at_utc=fired_at_utc,
                    response_at_utc=response_at_utc,
                    start_mono=start_mono,
                    business_code=config_err_code,
                    transport_cause=None,
                    shots_fired=shots_fired,
                )

            if slot_taken_code is not None:
                self._log.info("poll_result", status="lost", code=slot_taken_code)
                return self._make_result(
                    status="lost",
                    booking=None,
                    duplicates=tuple(duplicates),
                    fired_at_utc=fired_at_utc,
                    response_at_utc=response_at_utc,
                    start_mono=start_mono,
                    business_code=slot_taken_code,
                    transport_cause=None,
                    shots_fired=shots_fired,
                )

            if unknown_code is not None:
                self._log.info(
                    "poll_result",
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
                    shots_fired=shots_fired,
                )

            self._log.info(
                "poll_result",
                status="lost",
                transport_cause=transport_cause_seen,
            )
            return self._make_result(
                status="lost",
                booking=None,
                duplicates=tuple(duplicates),
                fired_at_utc=fired_at_utc,
                response_at_utc=response_at_utc,
                start_mono=start_mono,
                business_code=None,
                transport_cause=transport_cause_seen or "no_response",
                shots_fired=shots_fired,
            )
        finally:
            for task in pending:
                if not task.done():
                    task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _persist_win(self, booking: BookingResponse, court_id: int) -> None:
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
                phase="poll",
                booked_at_utc=self._clock.now_utc(),
            )
            await self._store.append(slot)
        except Exception:
            self._log.exception(
                "persistence_append_failed", record_id=booking.record_id
            )

    async def _cancel_duplicates(
        self,
        duplicates: list[BookingResponse],
        dup_courts: list[int | None],
    ) -> None:
        """Best-effort DELETE of stranded duplicates after a poll-fire win.
        Failures are logged with `cancel_response_status_code` and never raised
        — the main `won` outcome must always be returned to the caller.
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
        shots_fired: int,
    ) -> AttemptResult:
        duration_ms = (self._clock.monotonic() - start_mono) * 1000.0
        # `prearm_ok=False` — poll mode does not use prearm (slot state is
        # probed cheaply via search_timeslots, no TLS pre-warm needed).
        # Phase remains "poll" even for post_window_mode — AttemptPhase is a
        # narrow Literal["window", "poll"] union and persistence (BookedSlot)
        # already only knows about these two engine paths. The post-window
        # nuance is preserved in structlog bindings ("phase": "post_window_poll")
        # for telemetry without expanding the public type surface.
        return AttemptResult(
            status=status,
            booking=booking,
            duplicates=duplicates,
            fired_at_utc=fired_at_utc,
            response_at_utc=response_at_utc,
            duration_ms=duration_ms,
            business_code=business_code,
            transport_cause=transport_cause,
            prearm_ok=False,
            shots_fired=shots_fired,
            attempt_id=self._attempt_id,
            phase="poll",
        )
