from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import structlog

from tennis_booking.altegio import (
    AltegioBusinessError,
    AltegioClient,
    AltegioTransportError,
    BookingResponse,
)
from tennis_booking.common.clock import Clock
from tennis_booking.common.tz import ALMATY

from .codes import CONFIG_ERROR_CODES, NOT_OPEN_CODES, SLOT_TAKEN_CODES

__all__ = ["AttemptConfig", "AttemptResult", "BookingAttempt"]

_logger = structlog.get_logger(__name__)

AttemptStatus = Literal["won", "lost", "timeout", "error"]

_TIGHT_LOOP_LEAD_S = 1.0
_TIGHT_LOOP_STEP_S = 0.001
_PREARM_MIN_BUDGET_S = 0.1
_PREARM_TAIL_GUARD_S = 0.1
_PER_SHOT_TAIL_GUARD_S = 0.1
_PER_SHOT_MIN_TIMEOUT_S = 0.2


@dataclass(frozen=True)
class AttemptConfig:
    """Параметры одной попытки бронирования. Immutable — reused между prearm/fire/retry."""

    slot_dt_local: datetime
    court_id: int
    service_id: int
    fullname: str
    phone: str
    email: str | None = None
    parallel_shots: int = 2
    not_open_retry_ms: int = 100
    not_open_deadline_s: float = 5.0
    global_deadline_s: float = 10.0
    prearm_lead_s: float = 30.0

    def __post_init__(self) -> None:
        if self.slot_dt_local.tzinfo is None:
            raise ValueError("slot_dt_local must be timezone-aware")
        if self.slot_dt_local.tzinfo != ALMATY:
            raise ValueError(
                f"slot_dt_local must be in Asia/Almaty, got {self.slot_dt_local.tzinfo}"
            )
        if self.court_id < 1:
            raise ValueError(f"court_id must be >= 1, got {self.court_id}")
        if self.service_id < 1:
            raise ValueError(f"service_id must be >= 1, got {self.service_id}")
        if not self.fullname.strip():
            raise ValueError("fullname must not be empty after strip")
        if not self.phone.strip():
            raise ValueError("phone must not be empty after strip")
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


class BookingAttempt:
    """Одноразовый runner. Повторный run() → RuntimeError.

    State machine высокого уровня:
        (start) → validate_window → sleep_to_prearm → prearm
            → sleep_to_tight → tight_loop → fire (N shots)
            → retry_loop (not_open / transport) → (won | lost | timeout | error)
    """

    def __init__(self, config: AttemptConfig, client: AltegioClient, clock: Clock) -> None:
        self._config = config
        self._client = client
        self._clock = clock
        self._used = False
        self._attempt_id = uuid.uuid4().hex
        self._log = _logger.bind(
            attempt_id=self._attempt_id,
            slot_dt_local=config.slot_dt_local.isoformat(),
            court_id=config.court_id,
            dry_run=client.config.dry_run,
        )

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

    async def _fire_and_retry(
        self,
        *,
        fired_at_utc: datetime,
        start_mono: float,
        not_open_deadline_mono: float,
        deadline_at_mono: float,
        prearm_ok: bool,
    ) -> AttemptResult:
        pending: set[asyncio.Task[BookingResponse]] = set()
        task_idx: dict[asyncio.Task[BookingResponse], int] = {}
        shots_fired = 0
        response_at_utc: datetime | None = None
        duplicates: list[BookingResponse] = []

        for idx in range(self._config.parallel_shots):
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

                won_booking: BookingResponse | None = None
                not_open_retry_idxs: list[int] = []
                transport_retry_idxs: list[int] = []
                not_open_code_seen: str | None = None
                transport_cause_seen: str | None = None

                for task in done:
                    idx = task_idx.pop(task, -1)
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
                        if won_booking is None:
                            won_booking = booking
                        else:
                            duplicates.append(booking)
                        continue

                    if isinstance(exc, AltegioBusinessError):
                        self._log.info(
                            "response_received",
                            idx=idx,
                            status="business",
                            code=exc.code,
                            http_status=exc.http_status,
                        )
                        if exc.code in CONFIG_ERROR_CODES:
                            await self._cancel_all(pending)
                            self._log.info("result", status="error", code=exc.code)
                            return self._make_result(
                                status="error",
                                booking=None,
                                duplicates=tuple(duplicates),
                                fired_at_utc=fired_at_utc,
                                response_at_utc=response_at_utc,
                                start_mono=start_mono,
                                business_code=exc.code,
                                transport_cause=None,
                                prearm_ok=prearm_ok,
                                shots_fired=shots_fired,
                            )
                        if exc.code in SLOT_TAKEN_CODES:
                            await self._cancel_all(pending)
                            self._log.info("result", status="lost", code=exc.code)
                            return self._make_result(
                                status="lost",
                                booking=None,
                                duplicates=tuple(duplicates),
                                fired_at_utc=fired_at_utc,
                                response_at_utc=response_at_utc,
                                start_mono=start_mono,
                                business_code=exc.code,
                                transport_cause=None,
                                prearm_ok=prearm_ok,
                                shots_fired=shots_fired,
                            )
                        if exc.code in NOT_OPEN_CODES:
                            not_open_retry_idxs.append(idx)
                            not_open_code_seen = exc.code
                            continue
                        # Unknown business code → fallback to lost per PO decision.
                        await self._cancel_all(pending)
                        self._log.info(
                            "result", status="lost", code=exc.code, reason="unknown_code_fallback"
                        )
                        return self._make_result(
                            status="lost",
                            booking=None,
                            duplicates=tuple(duplicates),
                            fired_at_utc=fired_at_utc,
                            response_at_utc=response_at_utc,
                            start_mono=start_mono,
                            business_code=exc.code,
                            transport_cause=None,
                            prearm_ok=prearm_ok,
                            shots_fired=shots_fired,
                        )

                    if isinstance(exc, AltegioTransportError):
                        self._log.info(
                            "response_received",
                            idx=idx,
                            status="transport",
                            cause=exc.cause,
                        )
                        transport_retry_idxs.append(idx)
                        transport_cause_seen = exc.cause
                        continue

                    # Any other exception — unexpected; treat as transport-class retry.
                    self._log.warning(
                        "response_received",
                        idx=idx,
                        status="unknown_exception",
                        exc_type=type(exc).__name__,
                        error=str(exc),
                    )
                    transport_retry_idxs.append(idx)
                    transport_cause_seen = type(exc).__name__

                if won_booking is not None:
                    await self._drain_for_duplicates(pending, duplicates)
                    self._log.info(
                        "result",
                        status="won",
                        record_id=won_booking.record_id,
                        duplicates=len(duplicates),
                    )
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

                if not_open_retry_idxs:
                    now_mono = self._clock.monotonic()
                    if now_mono > not_open_deadline_mono:
                        await self._cancel_all(pending)
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
                    await self._clock.sleep(self._config.not_open_retry_ms / 1000.0)
                    for idx in not_open_retry_idxs:
                        task = self._spawn_shot(idx, deadline_at_mono)
                        pending.add(task)
                        task_idx[task] = idx
                        shots_fired += 1

                if transport_retry_idxs:
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
                    for idx in transport_retry_idxs:
                        task = self._spawn_shot(idx, deadline_at_mono)
                        pending.add(task)
                        task_idx[task] = idx
                        shots_fired += 1

            # pending empty: all shots finished but nothing decisive — treat as timeout.
            self._log.info("result", status="timeout", reason="no_pending_no_result")
            return self._make_result(
                status="timeout",
                booking=None,
                duplicates=tuple(duplicates),
                fired_at_utc=fired_at_utc,
                response_at_utc=response_at_utc,
                start_mono=start_mono,
                business_code=None,
                transport_cause=None,
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
        task = asyncio.create_task(self._shot(idx, timeout_s), name=f"shot-{idx}")
        self._log.info("shot_posted", idx=idx, timeout_s=timeout_s)
        return task

    async def _shot(self, idx: int, timeout_s: float) -> BookingResponse:
        return await self._client.create_booking(
            service_id=self._config.service_id,
            staff_id=self._config.court_id,
            slot_dt_local=self._config.slot_dt_local,
            fullname=self._config.fullname,
            phone=self._config.phone,
            email=self._config.email,
            timeout_s=timeout_s,
        )

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

    async def _drain_for_duplicates(
        self,
        pending: set[asyncio.Task[BookingResponse]],
        duplicates: list[BookingResponse],
    ) -> None:
        """После win — cancel остальные, но собрать уже-готовые success как duplicates."""
        if not pending:
            return
        # Cancel first to stop in-flight work.
        for task in pending:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*pending, return_exceptions=True)
        for task, res in zip(pending, results, strict=True):
            if task.cancelled():
                continue
            if isinstance(res, BookingResponse):
                duplicates.append(res)
        pending.clear()

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
        return AttemptResult(
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
        )
