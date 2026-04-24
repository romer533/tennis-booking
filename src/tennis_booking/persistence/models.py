from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from tennis_booking.common.tz import ALMATY

__all__ = ["BookedSlot", "BookingPhase", "PROFILE_NAME_RE", "SCHEMA_VERSION"]

SCHEMA_VERSION = 1

BookingPhase = Literal["window", "poll", "manual"]
_ALLOWED_PHASES: frozenset[str] = frozenset({"window", "poll", "manual"})

# Mirror config.schema.PROFILE_NAME_RE so a profile name from config can be
# round-tripped through persistence without separate validation logic.
PROFILE_NAME_RE = re.compile(r"^[a-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class BookedSlot:
    """Запись об одной успешной брони. Append-only.

    `slot_dt_local` — момент слота в Asia/Almaty (tz-aware). Канонично, чтобы
    дедуп работал между window/poll/manual независимо от того, как звонящий
    представляет время.

    `booked_at_utc` — момент, когда мы получили подтверждение от Altegio.
    Хранится в UTC, так избегаем путаницы при чтении логов из других TZ.

    Validation в `__post_init__` строгая: всё пришедшее извне (CLI, JSONL line)
    должно быть нормализовано к ALMATY/UTC до конструкции инстанса.
    """

    schema_version: int
    record_id: int
    record_hash: str
    slot_dt_local: datetime
    court_id: int
    service_id: int
    profile_name: str
    phase: BookingPhase
    booked_at_utc: datetime

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be {SCHEMA_VERSION}, got {self.schema_version}"
            )
        if self.record_id < 1:
            raise ValueError(f"record_id must be >= 1, got {self.record_id}")
        if not isinstance(self.record_hash, str) or not self.record_hash:
            raise ValueError("record_hash must be a non-empty string")
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
        if not isinstance(self.profile_name, str) or not PROFILE_NAME_RE.fullmatch(
            self.profile_name
        ):
            raise ValueError(
                f"profile_name must match [a-z0-9_-]+, got {self.profile_name!r}"
            )
        if self.phase not in _ALLOWED_PHASES:
            raise ValueError(
                f"phase must be one of {sorted(_ALLOWED_PHASES)}, got {self.phase!r}"
            )
        if self.booked_at_utc.tzinfo is None:
            raise ValueError("booked_at_utc must be timezone-aware")
        if self.booked_at_utc.utcoffset() != UTC.utcoffset(None):
            raise ValueError(
                f"booked_at_utc must be UTC (offset 0), got {self.booked_at_utc.tzinfo}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-ready dict. Datetimes are ISO 8601 with TZ-suffix."""
        return {
            "schema_version": self.schema_version,
            "record_id": self.record_id,
            "record_hash": self.record_hash,
            "slot_dt_local": self.slot_dt_local.isoformat(),
            "court_id": self.court_id,
            "service_id": self.service_id,
            "profile_name": self.profile_name,
            "phase": self.phase,
            "booked_at_utc": self.booked_at_utc.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BookedSlot:
        """Parse from JSONL line dict. Naive datetime → ValueError."""
        try:
            schema_version = data["schema_version"]
            record_id = data["record_id"]
            record_hash = data["record_hash"]
            slot_raw = data["slot_dt_local"]
            court_id = data["court_id"]
            service_id = data["service_id"]
            profile_name = data["profile_name"]
            phase = data["phase"]
            booked_raw = data["booked_at_utc"]
        except KeyError as e:
            raise ValueError(f"missing field in BookedSlot dict: {e.args[0]}") from e

        if not isinstance(slot_raw, str):
            raise ValueError(
                f"slot_dt_local must be ISO string, got {type(slot_raw).__name__}"
            )
        if not isinstance(booked_raw, str):
            raise ValueError(
                f"booked_at_utc must be ISO string, got {type(booked_raw).__name__}"
            )

        try:
            slot_dt = datetime.fromisoformat(slot_raw)
        except ValueError as e:
            raise ValueError(f"slot_dt_local is not ISO 8601: {slot_raw!r}") from e
        try:
            booked_dt = datetime.fromisoformat(booked_raw)
        except ValueError as e:
            raise ValueError(f"booked_at_utc is not ISO 8601: {booked_raw!r}") from e

        if slot_dt.tzinfo is None:
            raise ValueError(f"slot_dt_local must carry tzinfo, got naive: {slot_raw!r}")
        if booked_dt.tzinfo is None:
            raise ValueError(f"booked_at_utc must carry tzinfo, got naive: {booked_raw!r}")

        # Re-anchor to canonical zoneinfo objects: fromisoformat parses "+05:00"
        # as a fixed timezone, not Asia/Almaty. The dataclass validator compares
        # tzinfo by identity, so we must convert here.
        slot_almaty = slot_dt.astimezone(ALMATY)
        booked_utc = booked_dt.astimezone(UTC)

        return cls(
            schema_version=int(schema_version),
            record_id=int(record_id),
            record_hash=str(record_hash),
            slot_dt_local=slot_almaty,
            court_id=int(court_id),
            service_id=int(service_id),
            profile_name=str(profile_name),
            phase=phase,
            booked_at_utc=booked_utc,
        )
