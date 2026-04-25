from __future__ import annotations

import logging
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError

from .errors import ConfigError
from .schema import AppConfig, BookingRule, CourtPool, Profile, ResolvedBooking

SCHEDULE_FILENAME = "schedule.yaml"
PROFILES_FILENAME = "profiles.yaml"

logger = logging.getLogger(__name__)


def _read_yaml(path: Path, example_filename: str) -> Any:
    if not path.exists():
        raise ConfigError(
            f"{path.name} not found at {path}. "
            f"Copy from {example_filename} and edit."
        )
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError as e:
        raise ConfigError(f"failed to read {path}: {e}") from e
    try:
        return yaml.safe_load(text)
    except yaml.MarkedYAMLError as e:
        mark = e.problem_mark
        loc = (
            f" at line {mark.line + 1}, column {mark.column + 1}"
            if mark is not None
            else ""
        )
        raise ConfigError(
            f"invalid YAML in {path.name}{loc}: {e.problem or e}"
        ) from e
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {path.name}: {e}") from e


def _format_validation_error(filename: str, exc: ValidationError, context: str = "") -> str:
    lines = [f"invalid {filename}{(' (' + context + ')') if context else ''}:"]
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        msg = err["msg"]
        lines.append(f"  - {loc}: {msg}")
    return "\n".join(lines)


def load_profiles(path: Path) -> dict[str, Profile]:
    """Читает profiles.yaml и возвращает name → Profile."""
    raw = _read_yaml(path, "profiles.example.yaml")
    if raw is None:
        raise ConfigError(
            f"{path.name} is empty: at least one profile required"
        )
    if not isinstance(raw, dict):
        raise ConfigError(
            f"{path.name} root must be a mapping with 'profiles' key"
        )
    profiles_raw = raw.get("profiles")
    if profiles_raw is None:
        raise ConfigError(
            f"{path.name} must contain 'profiles' key with at least one profile"
        )
    if not isinstance(profiles_raw, dict):
        raise ConfigError(
            f"{path.name} 'profiles' must be a mapping of name → profile data"
        )
    if not profiles_raw:
        raise ConfigError(
            f"{path.name} has 0 profiles: at least one profile required"
        )

    extra_keys = set(raw.keys()) - {"profiles"}
    if extra_keys:
        raise ConfigError(
            f"{path.name}: unexpected top-level keys: {sorted(extra_keys)}"
        )

    result: dict[str, Profile] = {}
    for name, data in profiles_raw.items():
        if not isinstance(name, str):
            raise ConfigError(
                f"{path.name}: profile name must be a string, got {type(name).__name__}"
            )
        if not isinstance(data, dict):
            raise ConfigError(
                f"{path.name}: profile {name!r} must be a mapping, "
                f"got {type(data).__name__}"
            )
        try:
            profile = Profile(name=name, **data)
        except ValidationError as e:
            raise ConfigError(
                _format_validation_error(path.name, e, context=f"profile {name!r}")
            ) from e
        except TypeError as e:
            raise ConfigError(
                f"invalid {path.name} (profile {name!r}): {e}"
            ) from e
        result[name] = profile
    return result


def _load_court_pools_from_raw(
    raw: Any, filename: str
) -> dict[str, CourtPool]:
    """Парсит секцию court_pools (mapping name → {service_id, court_ids})."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"{filename} 'court_pools' must be a mapping of name → pool data, "
            f"got {type(raw).__name__}"
        )
    result: dict[str, CourtPool] = {}
    for name, data in raw.items():
        if not isinstance(name, str):
            raise ConfigError(
                f"{filename}: court_pool name must be a string, got {type(name).__name__}"
            )
        if not isinstance(data, dict):
            raise ConfigError(
                f"{filename}: court_pool {name!r} must be a mapping, "
                f"got {type(data).__name__}"
            )
        try:
            pool = CourtPool(**data)
        except ValidationError as e:
            raise ConfigError(
                _format_validation_error(filename, e, context=f"court_pool {name!r}")
            ) from e
        except TypeError as e:
            raise ConfigError(
                f"invalid {filename} (court_pool {name!r}): {e}"
            ) from e
        # Pool name must be valid identifier (regex same as in BookingRule.court_pool).
        from .schema import POOL_NAME_RE

        if not POOL_NAME_RE.fullmatch(name):
            raise ConfigError(
                f"{filename}: court_pool name must match [a-z0-9_-]+, got {name!r}"
            )
        result[name] = pool
    return result


def load_court_pools(path: Path) -> dict[str, CourtPool]:
    """Читает schedule.yaml и возвращает name → CourtPool. Пустой/отсутствующий
    section даёт пустой dict — допустимо для legacy schedule без pools.
    """
    raw = _read_yaml(path, "schedule.example.yaml")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"{path.name} root must be a mapping with 'bookings' key"
        )
    return _load_court_pools_from_raw(raw.get("court_pools"), path.name)


def load_schedule(path: Path) -> tuple[BookingRule, ...]:
    """Читает schedule.yaml и возвращает кортеж BookingRule."""
    raw = _read_yaml(path, "schedule.example.yaml")
    if raw is None:
        logger.warning("schedule has 0 bookings")
        return ()
    if not isinstance(raw, dict):
        raise ConfigError(
            f"{path.name} root must be a mapping with 'bookings' key"
        )
    bookings_raw = raw.get("bookings")
    if bookings_raw is None:
        logger.warning("schedule has 0 bookings")
        return ()

    extra_keys = set(raw.keys()) - {"bookings", "court_pools"}
    if extra_keys:
        raise ConfigError(
            f"{path.name}: unexpected top-level keys: {sorted(extra_keys)}"
        )

    if not isinstance(bookings_raw, list):
        raise ConfigError(
            f"{path.name} 'bookings' must be a list, got {type(bookings_raw).__name__}"
        )

    if not bookings_raw:
        logger.warning("schedule has 0 bookings")
        return ()

    rules: list[BookingRule] = []
    for idx, item in enumerate(bookings_raw):
        if not isinstance(item, dict):
            raise ConfigError(
                f"{path.name}: bookings[{idx}] must be a mapping, "
                f"got {type(item).__name__}"
            )
        try:
            rule = BookingRule(**item)
        except ValidationError as e:
            raise ConfigError(
                _format_validation_error(
                    path.name, e, context=f"bookings[{idx}]"
                )
            ) from e
        except TypeError as e:
            raise ConfigError(
                f"invalid {path.name} (bookings[{idx}]): {e}"
            ) from e
        rules.append(rule)
    return tuple(rules)


def _resolve(
    rules: tuple[BookingRule, ...],
    profiles: dict[str, Profile],
    pools: dict[str, CourtPool],
) -> tuple[ResolvedBooking, ...]:
    # Cross-validation: per-court dedup is done over EXPANDED court_ids, so a
    # legacy single-court booking conflicts with a pool-booking that contains
    # the same court at the same (weekday, slot).
    court_owner: dict[tuple[str, str, int], str] = {}
    resolved: list[ResolvedBooking] = []

    for rule in rules:
        if rule.profile not in profiles:
            raise ConfigError(
                f"booking {rule.name!r} references unknown profile "
                f"{rule.profile!r} (known profiles: {sorted(profiles.keys())})"
            )

        if rule.court_pool is not None:
            if rule.court_pool not in pools:
                raise ConfigError(
                    f"booking {rule.name!r} references unknown pool "
                    f"{rule.court_pool!r} (known pools: {sorted(pools.keys())})"
                )
            pool = pools[rule.court_pool]
            court_ids = pool.court_ids
            service_id = pool.service_id
            pool_name: str | None = rule.court_pool
        else:
            assert rule.court_id is not None  # XOR guarantees this
            assert rule.service_id is not None
            court_ids = (rule.court_id,)
            service_id = rule.service_id
            pool_name = None

        weekday_s = rule.weekday.value
        slot_s = rule.slot_local_time.strftime("%H:%M")
        for cid in court_ids:
            slot_key = (weekday_s, slot_s, cid)
            existing = court_owner.get(slot_key)
            if existing is not None:
                raise ConfigError(
                    f"duplicate booking slot ({weekday_s} {slot_s} "
                    f"court={cid}): {existing!r} and {rule.name!r}"
                )
            court_owner[slot_key] = rule.name

        resolved.append(
            ResolvedBooking(
                name=rule.name,
                weekday=rule.weekday,
                slot_local_time=rule.slot_local_time,
                duration_minutes=rule.duration_minutes,
                court_ids=court_ids,
                service_id=service_id,
                profile=profiles[rule.profile],
                enabled=rule.enabled,
                pool_name=pool_name,
                poll=rule.poll,
                grace_polling=rule.grace_polling,
            )
        )
    return tuple(resolved)


def load_app_config(config_dir: Path) -> AppConfig:
    """Главный API: читает schedule.yaml + profiles.yaml, валидирует, резолвит ссылки."""
    if not config_dir.exists():
        raise ConfigError(f"config directory not found: {config_dir}")
    if not config_dir.is_dir():
        raise ConfigError(f"config path is not a directory: {config_dir}")

    profiles = load_profiles(config_dir / PROFILES_FILENAME)
    schedule_path = config_dir / SCHEDULE_FILENAME
    pools = load_court_pools(schedule_path)
    rules = load_schedule(schedule_path)
    resolved = _resolve(rules, profiles, pools)

    referenced_profiles = {r.profile.name for r in resolved}
    for name in profiles:
        if name not in referenced_profiles:
            logger.warning("profile %r is defined but not referenced by any booking", name)

    referenced_pools = {r.pool_name for r in resolved if r.pool_name is not None}
    for name in pools:
        if name not in referenced_pools:
            logger.warning("court_pool %r is defined but not referenced by any booking", name)

    return AppConfig(
        bookings=resolved,
        profiles=MappingProxyType(dict(profiles)),
        court_pools=MappingProxyType(dict(pools)),
    )
