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


def test_load_booking_without_poll_resolves_poll_none(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "Пятница вечер"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    assert len(cfg.bookings) == 1
    assert cfg.bookings[0].poll is None


def test_load_booking_with_poll_resolves_through(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "Пятница вечер"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    poll:
      interval_s: 60
      start_offset_days: 2
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    rb = cfg.bookings[0]
    assert rb.poll is not None
    assert rb.poll.interval_s == 60
    assert rb.poll.start_offset_days == 2


def test_load_poll_interval_below_floor_rejected(tmp_path: Path) -> None:
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
      interval_s: 5
      start_offset_days: 2
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="interval_s"):
        load_app_config(tmp_path)


def test_load_poll_offset_zero_rejected(tmp_path: Path) -> None:
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
      start_offset_days: 0
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="start_offset_days"):
        load_app_config(tmp_path)


def test_load_poll_offset_above_max_rejected(tmp_path: Path) -> None:
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
      start_offset_days: 31
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="start_offset_days"):
        load_app_config(tmp_path)


def test_load_poll_extra_field_rejected(tmp_path: Path) -> None:
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
      stop_after_slot: true
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError):
        load_app_config(tmp_path)


def test_load_mixed_bookings_some_with_poll(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    poll:
      interval_s: 30
      start_offset_days: 1
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
    assert by_name["fri"].poll is not None
    assert by_name["fri"].poll.interval_s == 30
    assert by_name["sat"].poll is None


def test_load_poll_with_court_pool(tmp_path: Path) -> None:
    schedule = """\
court_pools:
  open:
    service_id: 7849893
    court_ids: [5, 6, 7]
bookings:
  - name: "evening"
    weekday: friday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: open
    profile: roman
    poll:
      interval_s: 45
      start_offset_days: 2
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    rb = cfg.bookings[0]
    assert rb.court_ids == (5, 6, 7)
    assert rb.poll is not None
    assert rb.poll.interval_s == 45


def test_load_poll_interval_floor_10_accepted(tmp_path: Path) -> None:
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
      interval_s: 10
      start_offset_days: 1
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    assert cfg.bookings[0].poll is not None
    assert cfg.bookings[0].poll.interval_s == 10


def test_load_poll_offset_max_30_accepted(tmp_path: Path) -> None:
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
      start_offset_days: 30
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    assert cfg.bookings[0].poll is not None
    assert cfg.bookings[0].poll.start_offset_days == 30
