from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any

import structlog

from tennis_booking.altegio.client import AltegioClient
from tennis_booking.common.clock import Clock, SystemClock
from tennis_booking.common.tz import ALMATY
from tennis_booking.config.schema import AppConfig, ResolvedBooking, Weekday
from tennis_booking.engine.attempt import (
    AttemptConfig,
    AttemptResult,
    BookingAttempt,
)
from tennis_booking.engine.poll import PollAttempt, PollConfigData
from tennis_booking.engine.poll_cache import PollResultCache
from tennis_booking.persistence import BookingStore
from tennis_booking.scheduler.clock import CheckResult, check_ntp_drift
from tennis_booking.scheduler.clock_errors import (
    ClockDriftError,
    NTPUnreachableError,
)
from tennis_booking.scheduler.window import next_open_window

__all__ = [
    "DEFAULT_MIN_LEAD_TIME_HOURS",
    "DEFAULT_NTP_THRESHOLD_MS",
    "DEFAULT_POST_WINDOW_POLL_INTERVAL_S",
    "LOOKAHEAD_WEEKS",
    "RECOMPUTE_LOCAL_TIME",
    "SHUTDOWN_TIMEOUT_S",
    "AttemptFactory",
    "NTPChecker",
    "PollAttemptFactory",
    "PostWindowPollFactory",
    "ScheduledAttempt",
    "SchedulerLoop",
]

RECOMPUTE_LOCAL_TIME = time(6, 55)
DEFAULT_NTP_THRESHOLD_MS = 50
SHUTDOWN_TIMEOUT_S = 60.0
# In-code default is 0.0 (guard disabled) so existing tests / dev runs are not
# silently broken. Production sets TENNIS_MIN_LEAD_TIME_HOURS=2 in __main__.py.
DEFAULT_MIN_LEAD_TIME_HOURS = 0.0
# Sanity bound: if the nearest weekly slot's window is already in the past,
# walk forward week-by-week looking for the first occurrence whose window is
# still open in the future. Anything beyond ~4 weeks indicates broken config,
# not a normal "we just missed today's window" situation.
LOOKAHEAD_WEEKS = 4
# Post-window poll keeps hunting for cancellations released by other users
# after the natural window phase has resolved with a loss. 120s tick keeps
# Altegio load conservative (one search_timeslots POST per 2 minutes) — this
# default was raised from 60s after PR #21's Cloudflare 403 incident, where
# any extra request volume was suspected. 30s is the floor for sanity.
DEFAULT_POST_WINDOW_POLL_INTERVAL_S = 120

_DAILY_INTERVAL_S = 24 * 60 * 60

_WEEKDAY_TO_INT: dict[Weekday, int] = {
    Weekday.MONDAY: 0,
    Weekday.TUESDAY: 1,
    Weekday.WEDNESDAY: 2,
    Weekday.THURSDAY: 3,
    Weekday.FRIDAY: 4,
    Weekday.SATURDAY: 5,
    Weekday.SUNDAY: 6,
}


@dataclass(frozen=True)
class ScheduledAttempt:
    booking: ResolvedBooking
    slot_dt_local: datetime
    window_open_utc: datetime


AttemptFactory = Callable[
    [AttemptConfig, AltegioClient, Clock, BookingStore | None], BookingAttempt
]
PollAttemptFactory = Callable[
    [
        AttemptConfig,
        PollConfigData,
        AltegioClient,
        Clock,
        asyncio.Event,
        BookingStore | None,
    ],
    PollAttempt,
]
PostWindowPollFactory = Callable[
    [
        AttemptConfig,
        PollConfigData,
        AltegioClient,
        Clock,
        asyncio.Event,
        BookingStore | None,
    ],
    PollAttempt,
]
NTPChecker = Callable[[], Awaitable[CheckResult]]


def _default_attempt_factory(
    config: AttemptConfig,
    client: AltegioClient,
    clock: Clock,
    store: BookingStore | None,
) -> BookingAttempt:
    return BookingAttempt(config, client, clock, store=store)


def _make_default_poll_attempt_factory(
    cache: PollResultCache | None,
) -> PollAttemptFactory:
    """Build the production PollAttempt factory. Closes over the loop's shared
    `PollResultCache` so every spawned PollAttempt consults the same cache —
    that is what collapses N polls per cycle to 1 fetch per (date, pool).
    """

    def _factory(
        config: AttemptConfig,
        poll: PollConfigData,
        client: AltegioClient,
        clock: Clock,
        won_event: asyncio.Event,
        store: BookingStore | None,
    ) -> PollAttempt:
        return PollAttempt(
            config,
            poll,
            client,
            clock,
            won_event=won_event,
            store=store,
            cache=cache,
            pool_key=config.pool_key,
        )

    return _factory


def _make_default_post_window_poll_factory(
    cache: PollResultCache | None,
) -> PostWindowPollFactory:
    def _factory(
        config: AttemptConfig,
        poll: PollConfigData,
        client: AltegioClient,
        clock: Clock,
        won_event: asyncio.Event,
        store: BookingStore | None,
    ) -> PollAttempt:
        return PollAttempt(
            config,
            poll,
            client,
            clock,
            won_event=won_event,
            store=store,
            post_window_mode=True,
            cache=cache,
            pool_key=config.pool_key,
        )

    return _factory


def _default_ntp_checker(threshold_ms: int) -> NTPChecker:
    async def _check() -> CheckResult:
        return await check_ntp_drift(threshold_ms=threshold_ms)

    return _check


# Suffix appended to the base scheduled key for poll-mode tasks. Window task
# uses the bare key (backward compat with existing dedup logic). The post-window
# poll runs after the window task has resolved (lost / timeout) and uses its
# own suffix so a re-recompute does not duplicate it onto the still-live key.
_POLL_KEY_SUFFIX = ":poll"
_POST_WINDOW_POLL_KEY_SUFFIX = ":post_window_poll"


def _scheduled_key(
    booking_name: str,
    slot_dt_local: datetime,
    court_ids: tuple[int, ...],
    service_id: int,
    suffix: str = "",
) -> tuple[str, str, int, int, str]:
    # Two BookingRule's can legally share a `name` (config loader dedupes by
    # (weekday, slot_time, court_id), not name) — for example "Вечер" on court 5
    # and "Вечер" on court 6 when the user wants either court. Keying only by
    # (name, slot) would silently drop one of them as a duplicate.
    # `court_ids` is a tuple (immutable) → hashable; hash is deterministic for
    # same tuple, so re-spawn after recompute keeps the same key.
    # `suffix` differentiates window-task ("") and poll-task (":poll") keys
    # for the same booking — both run concurrently, sharing won_event.
    return (booking_name, slot_dt_local.isoformat(), hash(court_ids), service_id, suffix)


class SchedulerLoop:
    """Главный daily-loop сервиса.

    Раз в сутки в `recompute_local_time` (Almaty) пересчитывает окна на ближайшие
    24ч и регистрирует задачу-аттемпт под каждое. Каждая задача спит до T-prearm
    (управляется `BookingAttempt.run`), затем стреляет.
    """

    def __init__(
        self,
        config: AppConfig,
        altegio_client: AltegioClient,
        clock: Clock | None = None,
        recompute_local_time: time = RECOMPUTE_LOCAL_TIME,
        ntp_required: bool = True,
        ntp_threshold_ms: int = DEFAULT_NTP_THRESHOLD_MS,
        attempt_factory: AttemptFactory | None = None,
        poll_attempt_factory: PollAttemptFactory | None = None,
        post_window_poll_factory: PostWindowPollFactory | None = None,
        ntp_checker: NTPChecker | None = None,
        shutdown_timeout_s: float = SHUTDOWN_TIMEOUT_S,
        store: BookingStore | None = None,
        min_lead_time_hours: float = DEFAULT_MIN_LEAD_TIME_HOURS,
        post_window_poll_interval_s: int = DEFAULT_POST_WINDOW_POLL_INTERVAL_S,
        post_window_poll_enabled: bool = True,
    ) -> None:
        if min_lead_time_hours < 0.0 or min_lead_time_hours > 168.0:
            raise ValueError(
                f"min_lead_time_hours must be in [0.0, 168.0], got {min_lead_time_hours}"
            )
        if post_window_poll_interval_s < 30:
            raise ValueError(
                f"post_window_poll_interval_s must be >= 30, got {post_window_poll_interval_s}"
            )
        self._config = config
        self._client = altegio_client
        self._clock: Clock = clock if clock is not None else SystemClock()
        self._recompute_local_time = recompute_local_time
        self._ntp_required = ntp_required
        self._ntp_threshold_ms = ntp_threshold_ms
        self._shutdown_timeout_s = shutdown_timeout_s
        self._store = store
        self._min_lead_time_hours = min_lead_time_hours
        self._post_window_poll_interval_s = post_window_poll_interval_s
        self._post_window_poll_enabled = post_window_poll_enabled
        self._attempt_factory: AttemptFactory = (
            attempt_factory if attempt_factory is not None else _default_attempt_factory
        )
        # Shared poll-result cache for the lifetime of this SchedulerLoop.
        # TTL == post_window_poll_interval_s (the default poll cadence). Both
        # the in-window poll and the post-window poll share the same cache;
        # this is intentional — they hit the SAME endpoint with the SAME
        # (date, pool) on overlapping schedules, and are exactly the
        # requests we need to coalesce.
        self._poll_cache: PollResultCache = PollResultCache(
            self._clock, ttl_s=float(post_window_poll_interval_s)
        )
        self._poll_attempt_factory: PollAttemptFactory = (
            poll_attempt_factory
            if poll_attempt_factory is not None
            else _make_default_poll_attempt_factory(self._poll_cache)
        )
        self._post_window_poll_factory: PostWindowPollFactory = (
            post_window_poll_factory
            if post_window_poll_factory is not None
            else _make_default_post_window_poll_factory(self._poll_cache)
        )
        self._ntp_checker: NTPChecker = (
            ntp_checker if ntp_checker is not None else _default_ntp_checker(ntp_threshold_ms)
        )

        self._loop_id = uuid.uuid4().hex
        self._log = structlog.get_logger("scheduler.loop").bind(loop_id=self._loop_id)

        self._scheduled: dict[tuple[str, str, int, int, str], asyncio.Task[None]] = {}
        # Tasks that crossed into the "running" (post-prearm) phase. On stop(), these
        # are awaited (with deadline) instead of being cancelled — losing a slot
        # mid-fire because of SIGTERM is the worst possible outcome.
        self._running: set[asyncio.Task[None]] = set()
        # Per-booking-occurrence shared event. Window-task and poll-task for the
        # same (booking, slot) coordinate via this — first to fire calls .set(),
        # the other observes and bails out before duplicating the booking.
        self._won_events: dict[tuple[str, str, int, int], asyncio.Event] = {}
        self._stop_event = asyncio.Event()
        self._stopped = False

    def _won_event_for(
        self,
        booking_name: str,
        slot_dt_local: datetime,
        court_ids: tuple[int, ...],
        service_id: int,
    ) -> asyncio.Event:
        key = (booking_name, slot_dt_local.isoformat(), hash(court_ids), service_id)
        evt = self._won_events.get(key)
        if evt is None:
            evt = asyncio.Event()
            self._won_events[key] = evt
        return evt

    # --- public API -----------------------------------------------------

    async def run(self) -> None:
        self._log.info("loop_starting", ntp_required=self._ntp_required)
        if not self._post_window_poll_enabled:
            # Logged once at startup (not on each booking). Operator wants to
            # immediately see in journalctl that the kill switch is in effect.
            self._log.info("post_window_poll_disabled")

        await self._startup_ntp_check()

        try:
            # First recompute fires immediately at startup, regardless of how
            # close `now` is to the daily recompute time. This avoids a tight
            # loop on exact-match start (06:55:00 → next_recompute_at == now,
            # delay 0, recompute, next_recompute_at == now again, ...).
            scheduled = await self._safe_recompute(self._clock.now_utc())
            self._spawn_attempts(scheduled)

            while not self._stop_event.is_set():
                now_utc = self._clock.now_utc()
                next_recompute_utc = self._next_recompute_at(now_utc)
                delay_s = (next_recompute_utc - now_utc).total_seconds()
                if delay_s < 0:
                    delay_s = 0.0
                await self._wait_or_stop(delay_s)
                if self._stop_event.is_set():
                    break
                scheduled = await self._safe_recompute(self._clock.now_utc())
                self._spawn_attempts(scheduled)
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self._stop_event.set()
        self._log.info(
            "loop_stopping",
            scheduled_count=len(self._scheduled),
            running_count=len(self._running),
        )

        # Snapshot — entries can mutate as tasks finalise during cancellation.
        all_tasks = list(self._scheduled.values())
        running_snapshot = set(self._running)

        for task in all_tasks:
            if task in running_snapshot:
                continue
            if not task.done():
                task.cancel()

        if all_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*all_tasks, return_exceptions=True),
                    timeout=self._shutdown_timeout_s,
                )
            except TimeoutError:
                self._log.warning("shutdown_timeout", timeout_s=self._shutdown_timeout_s)
                # Last-resort cancel for stuck running tasks.
                for task in all_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*all_tasks, return_exceptions=True)

        self._scheduled.clear()
        self._running.clear()
        self._log.info("loop_stopped")

    # --- recompute ------------------------------------------------------

    async def _safe_recompute(self, now_utc: datetime) -> list[ScheduledAttempt]:
        try:
            return await self._recompute_windows(now_utc)
        except Exception as e:  # noqa: BLE001 — recompute must not crash the loop
            self._log.exception("recompute_crashed", error=str(e), exc_type=type(e).__name__)
            return []

    async def _recompute_windows(self, now_utc: datetime) -> list[ScheduledAttempt]:
        self._log.info("recompute_started", bookings=len(self._config.bookings))
        result: list[ScheduledAttempt] = []
        for booking in self._config.bookings:
            if not booking.enabled:
                continue
            # Restart resilience: an occurrence whose window has passed but slot
            # is still in the future means we have nothing actively scheduled
            # for it (window task already exited at startup or before restart).
            # Spawn a post-window poll directly so a service restart between
            # T-2d 07:00 and T does not silently drop cancellation hunting.
            await self._maybe_restart_post_window_poll(booking, now_utc)

            nearest = self._next_slot_occurrence(
                now_utc, booking.weekday, booking.slot_local_time
            )
            slot_dt_local: datetime | None = None
            window_open_utc: datetime | None = None
            for week_offset in range(LOOKAHEAD_WEEKS):
                candidate = nearest + timedelta(days=7 * week_offset)
                candidate_window = next_open_window(candidate)
                if candidate_window >= now_utc:
                    slot_dt_local = candidate
                    window_open_utc = candidate_window
                    break
                self._log.warning(
                    "window_passed",
                    booking_name=booking.name,
                    slot_dt_local=candidate.isoformat(),
                    window_open_utc=candidate_window.isoformat(),
                    now_utc=now_utc.isoformat(),
                )
            if slot_dt_local is None or window_open_utc is None:
                self._log.error(
                    "no_future_window_found",
                    booking_name=booking.name,
                    weeks_searched=LOOKAHEAD_WEEKS,
                    now_utc=now_utc.isoformat(),
                )
                continue
            if self._store is not None:
                existing = await self._store.find(
                    slot_dt_local=slot_dt_local,
                    court_ids=list(booking.court_ids),
                    service_id=booking.service_id,
                    profile_name=booking.profile.name,
                )
                if existing is not None:
                    self._log.info(
                        "attempt_skipped_already_booked",
                        booking_name=booking.name,
                        slot_dt_local=slot_dt_local.isoformat(),
                        record_id=existing.record_id,
                        existing_phase=existing.phase,
                    )
                    continue
            result.append(
                ScheduledAttempt(
                    booking=booking,
                    slot_dt_local=slot_dt_local,
                    window_open_utc=window_open_utc,
                )
            )
        self._log.info("recompute_done", scheduled=len(result))
        return result

    async def _maybe_restart_post_window_poll(
        self, booking: ResolvedBooking, now_utc: datetime
    ) -> None:
        """Recompute-time restart resilience for post-window poll.

        For the nearest-future weekly occurrence of this booking: if its window
        has already opened (i.e. the window task either finished before restart
        or was never registered) and the slot is still > min_lead_time_hours
        away, no persistence record exists, and we don't already have a
        post-window task running for this slot — spawn one.

        Why "nearest-future" not "previous": `_next_slot_occurrence` returns
        the nearest *strictly future* slot. When that occurrence's window has
        already passed, this is exactly the slot for which post-window polling
        can still pay off (catch a cancellation between now and slot_dt). The
        regular recompute path skips it (window_open < now) and walks one week
        further; this method catches it.
        """
        if not self._post_window_poll_enabled:
            return
        candidate_slot = self._next_slot_occurrence(
            now_utc, booking.weekday, booking.slot_local_time
        )
        slot_utc = candidate_slot.astimezone(now_utc.tzinfo)

        window_open_utc = next_open_window(candidate_slot)
        if window_open_utc >= now_utc:
            # Window hasn't opened yet — handled by the regular path.
            return

        effective_min_lead = (
            booking.min_lead_time_hours
            if booking.min_lead_time_hours is not None
            else self._min_lead_time_hours
        )
        min_lead_s = effective_min_lead * 3600.0
        time_to_slot_s = (slot_utc - now_utc).total_seconds()
        if time_to_slot_s <= min_lead_s:
            return

        if self._store is not None:
            existing = await self._store.find(
                slot_dt_local=candidate_slot,
                court_ids=list(booking.court_ids),
                service_id=booking.service_id,
                profile_name=booking.profile.name,
            )
            if existing is not None:
                return

        post_window_key = _scheduled_key(
            booking.name,
            candidate_slot,
            booking.court_ids,
            booking.service_id,
            suffix=_POST_WINDOW_POLL_KEY_SUFFIX,
        )
        existing_task = self._scheduled.get(post_window_key)
        if existing_task is not None and not existing_task.done():
            return

        # If the window task is somehow still scheduled (e.g. recompute fired
        # before _wait_and_attempt completed), let the natural lost-window path
        # spawn the post-window poll instead — avoids racing two spawns.
        win_key = _scheduled_key(
            booking.name, candidate_slot, booking.court_ids, booking.service_id
        )
        if win_key in self._scheduled:
            return

        sa = ScheduledAttempt(
            booking=booking,
            slot_dt_local=candidate_slot,
            window_open_utc=window_open_utc,
        )
        self._log.info(
            "post_window_poll_restart_resilience",
            booking_name=booking.name,
            slot_dt_local=candidate_slot.isoformat(),
            time_to_slot_s=time_to_slot_s,
        )
        self._spawn_post_window_poll_task(sa)

    @staticmethod
    def _next_slot_occurrence(
        now_utc: datetime, weekday: Weekday, slot_time: time
    ) -> datetime:
        """Ближайший в строгом будущем datetime в Almaty с заданным weekday/time."""
        now_local = now_utc.astimezone(ALMATY)
        target_wd = _WEEKDAY_TO_INT[weekday]
        days_ahead = (target_wd - now_local.weekday()) % 7

        candidate = datetime(
            now_local.year,
            now_local.month,
            now_local.day,
            slot_time.hour,
            slot_time.minute,
            tzinfo=ALMATY,
        ) + timedelta(days=days_ahead)

        if candidate <= now_local:
            candidate += timedelta(days=7)
        return candidate

    # --- task spawning --------------------------------------------------

    def _spawn_attempts(self, scheduled: list[ScheduledAttempt]) -> None:
        for sa in scheduled:
            self._spawn_window_task(sa)
            if sa.booking.poll is not None:
                self._spawn_poll_task(sa)

    def _spawn_window_task(self, sa: ScheduledAttempt) -> None:
        key = _scheduled_key(
            sa.booking.name,
            sa.slot_dt_local,
            sa.booking.court_ids,
            sa.booking.service_id,
        )
        existing = self._scheduled.get(key)
        if existing is not None and not existing.done():
            self._log.info(
                "attempt_skipped_duplicate",
                booking_name=sa.booking.name,
                slot_dt_local=sa.slot_dt_local.isoformat(),
                phase="window",
            )
            return
        task = asyncio.create_task(
            self._wait_and_attempt(sa),
            name=f"attempt:{sa.booking.name}:{sa.slot_dt_local.isoformat()}",
        )
        self._scheduled[key] = task
        self._log.info(
            "attempt_scheduled",
            booking_name=sa.booking.name,
            court_ids=sa.booking.court_ids,
            pool_name=sa.booking.pool_name,
            slot_dt_local=sa.slot_dt_local.isoformat(),
            window_open_utc=sa.window_open_utc.isoformat(),
            phase="window",
        )

    def _spawn_poll_task(self, sa: ScheduledAttempt) -> None:
        assert sa.booking.poll is not None
        key = _scheduled_key(
            sa.booking.name,
            sa.slot_dt_local,
            sa.booking.court_ids,
            sa.booking.service_id,
            suffix=_POLL_KEY_SUFFIX,
        )
        existing = self._scheduled.get(key)
        if existing is not None and not existing.done():
            self._log.info(
                "attempt_skipped_duplicate",
                booking_name=sa.booking.name,
                slot_dt_local=sa.slot_dt_local.isoformat(),
                phase="poll",
            )
            return
        task = asyncio.create_task(
            self._wait_and_poll(sa),
            name=f"poll:{sa.booking.name}:{sa.slot_dt_local.isoformat()}",
        )
        self._scheduled[key] = task
        self._log.info(
            "attempt_scheduled",
            booking_name=sa.booking.name,
            court_ids=sa.booking.court_ids,
            pool_name=sa.booking.pool_name,
            slot_dt_local=sa.slot_dt_local.isoformat(),
            phase="poll",
            poll_interval_s=sa.booking.poll.interval_s,
            poll_start_offset_days=sa.booking.poll.start_offset_days,
        )

    async def _wait_and_attempt(self, scheduled: ScheduledAttempt) -> None:
        booking = scheduled.booking
        key = _scheduled_key(
            booking.name,
            scheduled.slot_dt_local,
            booking.court_ids,
            booking.service_id,
        )
        won_event = self._won_event_for(
            booking.name,
            scheduled.slot_dt_local,
            booking.court_ids,
            booking.service_id,
        )

        log = self._log.bind(
            booking_name=booking.name,
            court_ids=booking.court_ids,
            pool_name=booking.pool_name,
            slot_dt_local=scheduled.slot_dt_local.isoformat(),
            window_open_utc=scheduled.window_open_utc.isoformat(),
            phase="window",
        )

        try:
            attempt_cfg = self._build_attempt_config(scheduled)

            prearm_at_utc = scheduled.window_open_utc - timedelta(
                seconds=attempt_cfg.prearm_lead_s
            )
            now_utc = self._clock.now_utc()
            wait_to_prearm_s = (prearm_at_utc - now_utc).total_seconds()
            if wait_to_prearm_s > 0:
                await self._clock.sleep(wait_to_prearm_s)

            current_task = asyncio.current_task()
            if current_task is not None:
                self._running.add(current_task)

            if won_event.is_set():
                log.info("attempt_skipped_sibling_won")
                return

            if self._store is not None:
                existing = await self._store.find(
                    slot_dt_local=scheduled.slot_dt_local,
                    court_ids=list(booking.court_ids),
                    service_id=booking.service_id,
                    profile_name=booking.profile.name,
                )
                if existing is not None:
                    log.info(
                        "attempt_skipped_already_booked_at_prearm",
                        record_id=existing.record_id,
                        existing_phase=existing.phase,
                    )
                    return

            await self._pre_attempt_ntp_check(log)

            log.info("attempt_starting")
            attempt = self._attempt_factory(
                attempt_cfg, self._client, self._clock, self._store
            )
            try:
                result: AttemptResult = await attempt.run(scheduled.window_open_utc)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — engine is supposed to swallow these, but be defensive
                log.exception(
                    "attempt_crashed",
                    error=str(e),
                    exc_type=type(e).__name__,
                )
                return

            if result.status == "won":
                won_event.set()

            log.info(
                "attempt_finished",
                status=result.status,
                business_code=result.business_code,
                transport_cause=result.transport_cause,
                duration_ms=result.duration_ms,
                shots_fired=result.shots_fired,
                attempt_id=result.attempt_id,
                phase=result.phase,
            )

            # Window phase ended without a win → keep hunting for cancellations
            # released by other users between now and slot_dt_local. Status=error
            # is intentionally excluded — that signals a config / auth failure
            # which post-window polling cannot recover from.
            if result.status in ("lost", "timeout"):
                self._maybe_spawn_post_window_poll(scheduled, log)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — never let one task kill the loop
            log.exception(
                "attempt_crashed",
                error=str(e),
                exc_type=type(e).__name__,
            )
        finally:
            self._scheduled.pop(key, None)
            current_task = asyncio.current_task()
            if current_task is not None:
                self._running.discard(current_task)
            self._cleanup_won_event_if_done(
                booking.name,
                scheduled.slot_dt_local,
                booking.court_ids,
                booking.service_id,
            )

    async def _wait_and_poll(self, scheduled: ScheduledAttempt) -> None:
        booking = scheduled.booking
        assert booking.poll is not None
        key = _scheduled_key(
            booking.name,
            scheduled.slot_dt_local,
            booking.court_ids,
            booking.service_id,
            suffix=_POLL_KEY_SUFFIX,
        )
        won_event = self._won_event_for(
            booking.name,
            scheduled.slot_dt_local,
            booking.court_ids,
            booking.service_id,
        )

        log = self._log.bind(
            booking_name=booking.name,
            court_ids=booking.court_ids,
            pool_name=booking.pool_name,
            slot_dt_local=scheduled.slot_dt_local.isoformat(),
            phase="poll",
        )

        try:
            attempt_cfg = self._build_attempt_config(scheduled)
            poll_data = PollConfigData(
                interval_s=booking.poll.interval_s,
                start_offset_days=booking.poll.start_offset_days,
            )

            current_task = asyncio.current_task()
            if current_task is not None:
                self._running.add(current_task)

            if self._store is not None:
                existing = await self._store.find(
                    slot_dt_local=scheduled.slot_dt_local,
                    court_ids=list(booking.court_ids),
                    service_id=booking.service_id,
                    profile_name=booking.profile.name,
                )
                if existing is not None:
                    log.info(
                        "poll_skipped_already_booked",
                        record_id=existing.record_id,
                        existing_phase=existing.phase,
                    )
                    return

            log.info("poll_starting")
            attempt = self._poll_attempt_factory(
                attempt_cfg, poll_data, self._client, self._clock, won_event, self._store
            )
            try:
                result: AttemptResult = await attempt.run()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "attempt_crashed",
                    error=str(e),
                    exc_type=type(e).__name__,
                )
                return

            if result.status == "won":
                won_event.set()

            log.info(
                "attempt_finished",
                status=result.status,
                business_code=result.business_code,
                transport_cause=result.transport_cause,
                duration_ms=result.duration_ms,
                shots_fired=result.shots_fired,
                attempt_id=result.attempt_id,
                phase=result.phase,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.exception(
                "attempt_crashed",
                error=str(e),
                exc_type=type(e).__name__,
            )
        finally:
            self._scheduled.pop(key, None)
            current_task = asyncio.current_task()
            if current_task is not None:
                self._running.discard(current_task)
            self._cleanup_won_event_if_done(
                booking.name,
                scheduled.slot_dt_local,
                booking.court_ids,
                booking.service_id,
            )

    def _maybe_spawn_post_window_poll(
        self, scheduled: ScheduledAttempt, log: Any
    ) -> None:
        """Schedule a post-window poll task if the slot is still far enough away.

        Skipped when (slot - now) <= effective_min_lead — there is no useful
        polling window left and the fire-time guard would refuse any fire anyway.
        """
        if not self._post_window_poll_enabled:
            return
        booking = scheduled.booking
        effective_min_lead = (
            booking.min_lead_time_hours
            if booking.min_lead_time_hours is not None
            else self._min_lead_time_hours
        )
        slot_utc = scheduled.slot_dt_local.astimezone(self._clock.now_utc().tzinfo)
        time_to_slot_s = (slot_utc - self._clock.now_utc()).total_seconds()
        min_lead_s = effective_min_lead * 3600.0
        if time_to_slot_s <= min_lead_s:
            log.info(
                "post_window_poll_skipped",
                reason="too_close_to_slot",
                time_to_slot_s=time_to_slot_s,
                min_lead_s=min_lead_s,
            )
            return
        self._spawn_post_window_poll_task(scheduled)

    def _spawn_post_window_poll_task(self, sa: ScheduledAttempt) -> None:
        key = _scheduled_key(
            sa.booking.name,
            sa.slot_dt_local,
            sa.booking.court_ids,
            sa.booking.service_id,
            suffix=_POST_WINDOW_POLL_KEY_SUFFIX,
        )
        existing = self._scheduled.get(key)
        if existing is not None and not existing.done():
            self._log.info(
                "attempt_skipped_duplicate",
                booking_name=sa.booking.name,
                slot_dt_local=sa.slot_dt_local.isoformat(),
                phase="post_window_poll",
            )
            return
        task = asyncio.create_task(
            self._wait_and_post_window_poll(sa),
            name=f"post-window-poll:{sa.booking.name}:{sa.slot_dt_local.isoformat()}",
        )
        self._scheduled[key] = task
        self._log.info(
            "attempt_scheduled",
            booking_name=sa.booking.name,
            court_ids=sa.booking.court_ids,
            pool_name=sa.booking.pool_name,
            slot_dt_local=sa.slot_dt_local.isoformat(),
            phase="post_window_poll",
            interval_s=self._post_window_poll_interval_s,
        )

    async def _wait_and_post_window_poll(self, scheduled: ScheduledAttempt) -> None:
        booking = scheduled.booking
        key = _scheduled_key(
            booking.name,
            scheduled.slot_dt_local,
            booking.court_ids,
            booking.service_id,
            suffix=_POST_WINDOW_POLL_KEY_SUFFIX,
        )
        won_event = self._won_event_for(
            booking.name,
            scheduled.slot_dt_local,
            booking.court_ids,
            booking.service_id,
        )
        log = self._log.bind(
            booking_name=booking.name,
            court_ids=booking.court_ids,
            pool_name=booking.pool_name,
            slot_dt_local=scheduled.slot_dt_local.isoformat(),
            phase="post_window_poll",
        )

        try:
            attempt_cfg = self._build_attempt_config(scheduled)
            # PollConfigData.start_offset_days is required (>= 1) but is unused
            # in post_window_mode (effective_start = now). Pin to 1 — keeps the
            # validator happy without leaking mode-dependent semantics into the
            # config dataclass.
            poll_data = PollConfigData(
                interval_s=self._post_window_poll_interval_s,
                start_offset_days=1,
            )

            current_task = asyncio.current_task()
            if current_task is not None:
                self._running.add(current_task)

            if won_event.is_set():
                log.info("post_window_poll_skipped_sibling_won")
                return

            if self._store is not None:
                existing = await self._store.find(
                    slot_dt_local=scheduled.slot_dt_local,
                    court_ids=list(booking.court_ids),
                    service_id=booking.service_id,
                    profile_name=booking.profile.name,
                )
                if existing is not None:
                    log.info(
                        "post_window_poll_skipped_already_booked",
                        record_id=existing.record_id,
                        existing_phase=existing.phase,
                    )
                    return

            log.info("post_window_poll_starting")
            attempt = self._post_window_poll_factory(
                attempt_cfg, poll_data, self._client, self._clock, won_event, self._store
            )
            try:
                result: AttemptResult = await attempt.run()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.exception(
                    "attempt_crashed",
                    error=str(e),
                    exc_type=type(e).__name__,
                )
                return

            if result.status == "won":
                won_event.set()

            log.info(
                "attempt_finished",
                status=result.status,
                business_code=result.business_code,
                transport_cause=result.transport_cause,
                duration_ms=result.duration_ms,
                shots_fired=result.shots_fired,
                attempt_id=result.attempt_id,
                phase=result.phase,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.exception(
                "attempt_crashed",
                error=str(e),
                exc_type=type(e).__name__,
            )
        finally:
            self._scheduled.pop(key, None)
            current_task = asyncio.current_task()
            if current_task is not None:
                self._running.discard(current_task)
            self._cleanup_won_event_if_done(
                booking.name,
                scheduled.slot_dt_local,
                booking.court_ids,
                booking.service_id,
            )

    def _cleanup_won_event_if_done(
        self,
        booking_name: str,
        slot_dt_local: datetime,
        court_ids: tuple[int, ...],
        service_id: int,
    ) -> None:
        """Remove the shared won_event once all task variants (window, poll,
        post-window poll) for this (booking, slot) have exited. Prevents
        indefinite growth across recomputes.
        """
        win_key = _scheduled_key(booking_name, slot_dt_local, court_ids, service_id)
        poll_key = _scheduled_key(
            booking_name, slot_dt_local, court_ids, service_id, suffix=_POLL_KEY_SUFFIX
        )
        post_window_key = _scheduled_key(
            booking_name,
            slot_dt_local,
            court_ids,
            service_id,
            suffix=_POST_WINDOW_POLL_KEY_SUFFIX,
        )
        if (
            win_key in self._scheduled
            or poll_key in self._scheduled
            or post_window_key in self._scheduled
        ):
            return
        evt_key = (booking_name, slot_dt_local.isoformat(), hash(court_ids), service_id)
        self._won_events.pop(evt_key, None)

    async def _pre_attempt_ntp_check(self, log: Any) -> None:
        try:
            check = await self._ntp_checker()
        except ClockDriftError as e:
            log.warning(
                "pre_attempt_ntp_warn",
                drift_ms=e.drift_ms,
                threshold_ms=e.threshold_ms,
                server=e.server,
            )
            return
        except NTPUnreachableError as e:
            log.warning("pre_attempt_ntp_warn", server=e.server, cause=str(e))
            return
        except Exception as e:  # noqa: BLE001 — NTP is best-effort here
            log.warning(
                "pre_attempt_ntp_warn",
                cause=str(e),
                exc_type=type(e).__name__,
            )
            return

        if abs(check.drift_ms) > self._ntp_threshold_ms:
            log.warning(
                "pre_attempt_ntp_warn",
                drift_ms=check.drift_ms,
                threshold_ms=self._ntp_threshold_ms,
                server=check.server,
            )

    def _build_attempt_config(self, scheduled: ScheduledAttempt) -> AttemptConfig:
        booking = scheduled.booking
        # Per-booking override wins; otherwise app default (env-driven).
        effective_min_lead = (
            booking.min_lead_time_hours
            if booking.min_lead_time_hours is not None
            else self._min_lead_time_hours
        )
        return AttemptConfig(
            slot_dt_local=scheduled.slot_dt_local,
            court_ids=booking.court_ids,
            service_id=booking.service_id,
            fullname=booking.profile.full_name,
            phone=booking.profile.phone,
            profile_name=booking.profile.name,
            email=booking.profile.email,
            min_lead_time_hours=effective_min_lead,
            pool_key=booking.pool_name,
        )

    # --- timing helpers -------------------------------------------------

    def _next_recompute_at(self, now_utc: datetime) -> datetime:
        """UTC момент следующего recompute строго в будущем (либо ровно сейчас,
        если сегодняшний recompute ещё впереди).

        Если `now` >= сегодняшнего recompute (включая exact match) — возвращаем
        завтрашний recompute. Exact match → tomorrow гарантирует, что после
        выполнения recompute в 06:55:00 цикл не вычислит delay==0 и не сорвётся
        в tight-loop на FakeClock без advance(). Первый recompute при старте
        run() выполняется отдельно (см. `run`).
        """
        now_local = now_utc.astimezone(ALMATY)
        today_recompute = datetime(
            now_local.year,
            now_local.month,
            now_local.day,
            self._recompute_local_time.hour,
            self._recompute_local_time.minute,
            tzinfo=ALMATY,
        )
        if now_local < today_recompute:
            target_local = today_recompute
        else:
            target_local = today_recompute + timedelta(days=1)
        return target_local.astimezone(now_utc.tzinfo)

    async def _wait_or_stop(self, delay_s: float) -> None:
        if delay_s <= 0:
            # Yield once so a pending stop() can be observed before next recompute.
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_s)
        except TimeoutError:
            return

    # --- ntp startup ----------------------------------------------------

    async def _startup_ntp_check(self) -> None:
        try:
            check = await self._ntp_checker()
        except ClockDriftError as e:
            if self._ntp_required:
                self._log.error(
                    "ntp_check_failed",
                    cause="drift",
                    drift_ms=e.drift_ms,
                    threshold_ms=e.threshold_ms,
                    server=e.server,
                )
                raise
            self._log.warning(
                "ntp_check_failed",
                cause="drift",
                drift_ms=e.drift_ms,
                threshold_ms=e.threshold_ms,
                server=e.server,
                ntp_required=False,
            )
            return
        except NTPUnreachableError as e:
            if self._ntp_required:
                self._log.error(
                    "ntp_check_failed",
                    cause="unreachable",
                    server=e.server,
                    error=str(e),
                )
                raise
            self._log.warning(
                "ntp_check_failed",
                cause="unreachable",
                server=e.server,
                error=str(e),
                ntp_required=False,
            )
            return

        self._log.info(
            "ntp_check_ok",
            drift_ms=check.drift_ms,
            rtt_ms=check.rtt_ms,
            server=check.server,
        )
