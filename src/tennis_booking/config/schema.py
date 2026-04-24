from __future__ import annotations

import re
from datetime import time
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

PROFILE_NAME_RE = re.compile(r"^[a-z0-9_-]+$")
SLOT_TIME_RE = re.compile(r"^[0-2][0-9]:[0-5][0-9]$")
POOL_NAME_RE = re.compile(r"^[a-z0-9_-]+$")


class Weekday(StrEnum):
    MONDAY = "monday"
    TUESDAY = "tuesday"
    WEDNESDAY = "wednesday"
    THURSDAY = "thursday"
    FRIDAY = "friday"
    SATURDAY = "saturday"
    SUNDAY = "sunday"


def _parse_slot_time(value: Any) -> time:
    if not isinstance(value, str):
        raise ValueError(
            "slot_local_time must be a string in 'HH:MM' format "
            "(quote the value in YAML, e.g. \"07:00\")"
        )
    if not SLOT_TIME_RE.fullmatch(value):
        raise ValueError(
            f"slot_local_time must match 'HH:MM' (00:00–23:59), got {value!r}"
        )
    hour = int(value[:2])
    minute = int(value[3:])
    if hour > 23:
        raise ValueError(f"slot_local_time hour must be 00–23, got {value!r}")
    return time(hour, minute)


class Profile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    name: str
    full_name: str
    phone: str
    email: str | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not PROFILE_NAME_RE.fullmatch(v):
            raise ValueError(
                f"profile name must match [a-z0-9_-]+, got {v!r}"
            )
        return v

    @field_validator("full_name")
    @classmethod
    def _validate_full_name(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("full_name must not be empty")
        return stripped

    @field_validator("phone")
    @classmethod
    def _validate_phone(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("phone must not be empty")
        return stripped

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email(cls, v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    def __repr__(self) -> str:
        return f"<profile:{self.name}>"

    def __str__(self) -> str:
        return f"<profile:{self.name}>"


class CourtPool(BaseModel):
    """Группа кортов одного service_id. Одна booking-запись с court_pool: <name>
    будет fan-out'иться параллельными shots на все courts в пуле; first-success
    cancels остальные.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    service_id: int = Field(ge=1)
    court_ids: tuple[int, ...]

    @field_validator("court_ids", mode="before")
    @classmethod
    def _coerce_court_ids(cls, v: Any) -> Any:
        if isinstance(v, list):
            return tuple(v)
        return v

    @field_validator("court_ids")
    @classmethod
    def _validate_court_ids(cls, v: tuple[int, ...]) -> tuple[int, ...]:
        if not v:
            raise ValueError("court_ids must contain at least one id")
        for cid in v:
            if not isinstance(cid, int) or isinstance(cid, bool):
                raise ValueError(
                    f"court_ids entries must be integers, got {type(cid).__name__}"
                )
            if cid < 1:
                raise ValueError(f"court_ids entries must be >= 1, got {cid}")
        if len(set(v)) != len(v):
            raise ValueError(f"court_ids must be unique, got duplicates in {list(v)}")
        return v


class BookingRule(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    name: str
    weekday: Weekday
    slot_local_time: time
    duration_minutes: int = Field(ge=1, le=240)
    court_id: int | None = None
    service_id: int | None = None
    court_pool: str | None = None
    profile: str
    enabled: bool = True

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("booking name must not be empty")
        return stripped

    @field_validator("weekday", mode="before")
    @classmethod
    def _coerce_weekday(cls, v: Any) -> Any:
        if isinstance(v, Weekday):
            return v
        if isinstance(v, str):
            try:
                return Weekday(v)
            except ValueError as e:
                allowed = ", ".join(w.value for w in Weekday)
                raise ValueError(
                    f"weekday must be one of [{allowed}], got {v!r}"
                ) from e
        raise ValueError(f"weekday must be a string, got {type(v).__name__}")

    @field_validator("slot_local_time", mode="before")
    @classmethod
    def _validate_slot_local_time(cls, v: Any) -> time:
        return _parse_slot_time(v)

    @field_validator("court_id")
    @classmethod
    def _validate_court_id(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if v < 1:
            raise ValueError(f"court_id must be >= 1, got {v}")
        return v

    @field_validator("service_id")
    @classmethod
    def _validate_service_id(cls, v: int | None) -> int | None:
        if v is None:
            return None
        if v < 1:
            raise ValueError("service_id must be positive integer")
        return v

    @field_validator("court_pool")
    @classmethod
    def _validate_court_pool(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not POOL_NAME_RE.fullmatch(v):
            raise ValueError(
                f"court_pool name must match [a-z0-9_-]+, got {v!r}"
            )
        return v

    @field_validator("profile")
    @classmethod
    def _validate_profile_ref(cls, v: str) -> str:
        if not PROFILE_NAME_RE.fullmatch(v):
            raise ValueError(
                f"profile reference must match [a-z0-9_-]+, got {v!r}"
            )
        return v

    @model_validator(mode="after")
    def _validate_target(self) -> BookingRule:
        if self.court_pool is not None:
            if self.court_id is not None or self.service_id is not None:
                raise ValueError(
                    "booking must specify either court_pool OR (court_id + service_id), "
                    "not both"
                )
        else:
            if self.court_id is None and self.service_id is None:
                raise ValueError(
                    "booking must specify either court_pool, or both court_id and service_id"
                )
            if self.court_id is None or self.service_id is None:
                raise ValueError(
                    "booking must specify BOTH court_id and service_id (or use court_pool)"
                )
        return self


class ResolvedBooking(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, arbitrary_types_allowed=False)

    name: str
    weekday: Weekday
    slot_local_time: time
    duration_minutes: int
    court_ids: tuple[int, ...]
    service_id: int
    profile: Profile
    enabled: bool
    pool_name: str | None = None

    @field_validator("court_ids", mode="before")
    @classmethod
    def _coerce_court_ids(cls, v: Any) -> Any:
        if isinstance(v, list):
            return tuple(v)
        return v

    @field_validator("court_ids")
    @classmethod
    def _validate_court_ids(cls, v: tuple[int, ...]) -> tuple[int, ...]:
        if not v:
            raise ValueError("court_ids must not be empty")
        for cid in v:
            if not isinstance(cid, int) or isinstance(cid, bool):
                raise ValueError(
                    f"court_ids entries must be integers, got {type(cid).__name__}"
                )
            if cid < 1:
                raise ValueError(f"court_ids entries must be >= 1, got {cid}")
        if len(set(v)) != len(v):
            raise ValueError(f"court_ids must be unique, got duplicates in {list(v)}")
        return v

    @field_validator("service_id")
    @classmethod
    def _validate_service_id(cls, v: int) -> int:
        if v < 1:
            raise ValueError("service_id must be positive integer")
        return v

    def __repr__(self) -> str:
        if len(self.court_ids) <= 3:
            courts_s = "[" + ",".join(str(c) for c in self.court_ids) + "]"
        else:
            first = self.court_ids[0]
            extra = len(self.court_ids) - 1
            courts_s = f"[{first},+{extra} more]"
        pool_s = f" pool={self.pool_name}" if self.pool_name is not None else ""
        return (
            f"<booking:{self.name!r} weekday={self.weekday.value} "
            f"courts={courts_s}{pool_s} profile={self.profile!r}>"
        )

    def __str__(self) -> str:
        return self.__repr__()


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, arbitrary_types_allowed=True)

    bookings: tuple[ResolvedBooking, ...]
    profiles: MappingProxyType[str, Profile]
    court_pools: MappingProxyType[str, CourtPool]

    @field_validator("profiles", mode="before")
    @classmethod
    def _wrap_profiles(cls, v: Any) -> MappingProxyType[str, Profile]:
        if isinstance(v, MappingProxyType):
            return v
        if isinstance(v, dict):
            return MappingProxyType(dict(v))
        raise ValueError("profiles must be a mapping")

    @field_validator("court_pools", mode="before")
    @classmethod
    def _wrap_court_pools(cls, v: Any) -> MappingProxyType[str, CourtPool]:
        if v is None:
            return MappingProxyType({})
        if isinstance(v, MappingProxyType):
            return v
        if isinstance(v, dict):
            return MappingProxyType(dict(v))
        raise ValueError("court_pools must be a mapping")

    def __repr__(self) -> str:
        n_b = len(self.bookings)
        n_p = len(self.profiles)
        n_cp = len(self.court_pools)
        return (
            f"<AppConfig: {n_b} booking{'s' if n_b != 1 else ''}, "
            f"{n_p} profile{'s' if n_p != 1 else ''}, "
            f"{n_cp} court_pool{'s' if n_cp != 1 else ''}>"
        )

    def __str__(self) -> str:
        return self.__repr__()


def mask_phone(phone: str) -> str:
    """Маскирует телефон для логов: первые 4 + *** + последние 4."""
    if len(phone) <= 8:
        return "***"
    return f"{phone[:4]}***{phone[-4:]}"


def mask_email(email: str) -> str:
    """Маскирует email для логов: первая буква + *** + домен."""
    if "@" not in email:
        return "***"
    local, _, domain = email.partition("@")
    if not local:
        return f"***@{domain}"
    return f"{local[0]}***@{domain}"
