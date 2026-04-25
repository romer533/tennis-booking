"""Тесты loader: grace_polling pass-through через ResolvedBooking."""
from __future__ import annotations

from pathlib import Path

import pytest

from tennis_booking.config import ConfigError, load_app_config

GOOD_PROFILES = """\
profiles:
  roman:
    full_name: "Иванов Иван Иванович"
    phone: "+77001234567"
    email: "test@example.com"
"""


def _write(tmp_path: Path, profiles: str, schedule: str) -> Path:
    (tmp_path / "profiles.yaml").write_text(profiles, encoding="utf-8")
    (tmp_path / "schedule.yaml").write_text(schedule, encoding="utf-8")
    return tmp_path


def test_load_booking_without_grace_polling(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    assert cfg.bookings[0].grace_polling is None


def test_load_booking_with_grace_polling(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    grace_polling:
      period_s: 300
      interval_s: 15
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    rb = cfg.bookings[0]
    assert rb.grace_polling is not None
    assert rb.grace_polling.period_s == 300
    assert rb.grace_polling.interval_s == 15


def test_load_grace_polling_with_court_pool(tmp_path: Path) -> None:
    schedule = """\
court_pools:
  open:
    service_id: 7849893
    court_ids: [5, 6]
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: open
    profile: roman
    grace_polling:
      period_s: 120
      interval_s: 10
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    rb = cfg.bookings[0]
    assert rb.court_ids == (5, 6)
    assert rb.grace_polling is not None
    assert rb.grace_polling.period_s == 120


def test_load_grace_polling_period_below_min_rejected(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    grace_polling:
      period_s: 30
      interval_s: 10
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="period_s"):
        load_app_config(tmp_path)


def test_load_grace_polling_interval_above_max_rejected(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    grace_polling:
      period_s: 600
      interval_s: 400
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="interval_s"):
        load_app_config(tmp_path)


def test_load_grace_polling_extra_field_rejected(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    grace_polling:
      period_s: 120
      interval_s: 10
      foo: bar
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError):
        load_app_config(tmp_path)


def test_load_grace_polling_alongside_poll(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    poll:
      interval_s: 60
      start_offset_days: 2
    grace_polling:
      period_s: 120
      interval_s: 10
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    rb = cfg.bookings[0]
    assert rb.poll is not None
    assert rb.grace_polling is not None


def test_load_mixed_bookings_some_with_grace_polling(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    grace_polling:
      period_s: 240
      interval_s: 20
  - name: "sat"
    weekday: saturday
    slot_local_time: "10:00"
    duration_minutes: 60
    court_id: 6
    service_id: 7849893
    profile: roman
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    by_name = {rb.name: rb for rb in cfg.bookings}
    assert by_name["fri"].grace_polling is not None
    assert by_name["fri"].grace_polling.period_s == 240
    assert by_name["sat"].grace_polling is None
