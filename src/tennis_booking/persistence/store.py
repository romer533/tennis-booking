from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

import structlog

from .models import BookedSlot

__all__ = ["BookingStore", "FileBookingStore", "MemoryBookingStore"]

_logger = structlog.get_logger(__name__)


@runtime_checkable
class BookingStore(Protocol):
    """Append-only store of successful bookings, plus dedup lookup.

    Implementations MUST be safe to call concurrently from multiple tasks
    (the loop fans out per-booking tasks; each can independently invoke
    `find` / `append`).
    """

    async def append(self, slot: BookedSlot) -> None: ...

    async def find(
        self,
        slot_dt_local: datetime,
        court_ids: list[int],
        service_id: int,
        profile_name: str,
    ) -> BookedSlot | None: ...

    async def all_for_profile(self, profile_name: str) -> list[BookedSlot]: ...


def _matches(
    slot: BookedSlot,
    *,
    slot_dt_local: datetime,
    court_ids_set: set[int],
    service_id: int,
    profile_name: str,
) -> bool:
    if slot.profile_name != profile_name:
        return False
    if slot.service_id != service_id:
        return False
    if slot.court_id not in court_ids_set:
        return False
    # Compare in absolute terms so the caller can pass slot_dt_local in any
    # tz-aware form; canonical store value is in Almaty.
    return slot.slot_dt_local == slot_dt_local


class FileBookingStore:
    """JSONL append-only store on local disk.

    File format: one JSON object per line, no trailing comma, UTF-8.
    Writes are serialised through an asyncio.Lock; each write opens, appends,
    flushes, fsyncs and closes the file (no long-lived FD — keeps lock
    granularity tight and avoids losing data on rotation/external move).

    `find` / `all_for_profile` perform a full file scan. This is acceptable
    for the expected scale (≤ a few thousand records over the bot's lifetime,
    and lookups happen O(once) per booking per recompute).
    """

    def __init__(self, path: Path) -> None:
        if not isinstance(path, Path):
            raise ValueError(f"path must be Path, got {type(path).__name__}")
        parent = path.parent
        if not parent.exists() or not parent.is_dir():
            raise ValueError(
                f"FileBookingStore parent directory does not exist: {parent}"
            )
        self._path = path
        self._lock = asyncio.Lock()
        self._log = _logger.bind(store_path=str(path))

    @property
    def path(self) -> Path:
        return self._path

    async def append(self, slot: BookedSlot) -> None:
        line = json.dumps(slot.to_dict(), ensure_ascii=False, separators=(",", ":"))
        async with self._lock:
            await asyncio.to_thread(self._append_sync, line)
        self._log.info(
            "booking_persisted",
            record_id=slot.record_id,
            court_id=slot.court_id,
            phase=slot.phase,
            profile_name=slot.profile_name,
            slot_dt_local=slot.slot_dt_local.isoformat(),
        )

    def _append_sync(self, line: str) -> None:
        # `os.fsync` после каждой записи — критично: SIGTERM / kill -9 / power
        # loss между append и flush — потерянная запись = double-booking при
        # рестарте. Stoимость одного fsync (~ms) пренебрежимо мала на фоне
        # того, что append вызывается раз на успех (≤ единиц в день).
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(line)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

    async def find(
        self,
        slot_dt_local: datetime,
        court_ids: list[int],
        service_id: int,
        profile_name: str,
    ) -> BookedSlot | None:
        if not court_ids:
            return None
        court_set = set(court_ids)
        records = await asyncio.to_thread(self._load_all)
        for slot in records:
            if _matches(
                slot,
                slot_dt_local=slot_dt_local,
                court_ids_set=court_set,
                service_id=service_id,
                profile_name=profile_name,
            ):
                return slot
        return None

    async def all_for_profile(self, profile_name: str) -> list[BookedSlot]:
        records = await asyncio.to_thread(self._load_all)
        return [s for s in records if s.profile_name == profile_name]

    def _load_all(self) -> list[BookedSlot]:
        if not self._path.exists():
            return []
        result: list[BookedSlot] = []
        with open(self._path, encoding="utf-8") as f:
            for line_no, raw in enumerate(f, start=1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError as e:
                    self._log.warning(
                        "store_line_skip_json_error",
                        line_no=line_no,
                        error=str(e),
                    )
                    continue
                if not isinstance(data, dict):
                    self._log.warning(
                        "store_line_skip_not_object",
                        line_no=line_no,
                        type=type(data).__name__,
                    )
                    continue
                try:
                    slot = BookedSlot.from_dict(data)
                except (ValueError, KeyError, TypeError) as e:
                    self._log.warning(
                        "store_line_skip_invalid",
                        line_no=line_no,
                        error=str(e),
                    )
                    continue
                result.append(slot)
        return result


class MemoryBookingStore:
    """In-memory store for tests. Same API contract as FileBookingStore."""

    def __init__(self) -> None:
        self._records: list[BookedSlot] = []
        self._lock = asyncio.Lock()

    async def append(self, slot: BookedSlot) -> None:
        async with self._lock:
            self._records.append(slot)

    async def find(
        self,
        slot_dt_local: datetime,
        court_ids: list[int],
        service_id: int,
        profile_name: str,
    ) -> BookedSlot | None:
        if not court_ids:
            return None
        court_set = set(court_ids)
        async with self._lock:
            snapshot = list(self._records)
        for slot in snapshot:
            if _matches(
                slot,
                slot_dt_local=slot_dt_local,
                court_ids_set=court_set,
                service_id=service_id,
                profile_name=profile_name,
            ):
                return slot
        return None

    async def all_for_profile(self, profile_name: str) -> list[BookedSlot]:
        async with self._lock:
            snapshot = list(self._records)
        return [s for s in snapshot if s.profile_name == profile_name]
