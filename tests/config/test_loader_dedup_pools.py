"""Loader cross-validation tests: dedup over expanded court_ids covers
pool/legacy collisions on the same (weekday, slot)."""
from __future__ import annotations

from pathlib import Path

import pytest

from tennis_booking.config import ConfigError, load_app_config

GOOD_PROFILES = """\
profiles:
  roman:
    full_name: "R G"
    phone: "+77001234567"
"""


def write(tmp_path: Path, schedule: str) -> Path:
    (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
    (tmp_path / "schedule.yaml").write_text(schedule, encoding="utf-8")
    return tmp_path


class TestDedupAcrossPoolsAndLegacy:
    def test_pool_and_legacy_overlap_same_slot_rejected(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  outdoor:
    service_id: 7849893
    court_ids: [1521564, 1521565, 1521566, 1521567]
bookings:
  - name: "pool_b"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
  - name: "legacy_b"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 1521566
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError) as exc:
            load_app_config(tmp_path)
        msg = str(exc.value)
        assert "1521566" in msg
        assert "pool_b" in msg
        assert "legacy_b" in msg

    def test_two_pools_overlap_same_slot_rejected(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  a:
    service_id: 7849893
    court_ids: [1, 2, 3]
  b:
    service_id: 7849893
    court_ids: [3, 4, 5]
bookings:
  - name: "first"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: a
    profile: roman
  - name: "second"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: b
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError) as exc:
            load_app_config(tmp_path)
        assert "court=3" in str(exc.value)

    def test_pool_and_legacy_different_court_ok(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  outdoor:
    service_id: 7849893
    court_ids: [10, 11, 12]
bookings:
  - name: "pool_b"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
  - name: "legacy_b"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 999
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2

    def test_pool_and_legacy_different_slot_ok(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  outdoor:
    service_id: 7849893
    court_ids: [10, 11]
bookings:
  - name: "pool_b"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
  - name: "legacy_b"
    weekday: friday
    slot_local_time: "19:00"
    duration_minutes: 60
    court_id: 10
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2

    def test_pool_overlapping_with_disabled_legacy_still_rejected(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  outdoor:
    service_id: 7849893
    court_ids: [10, 11]
bookings:
  - name: "pool_b"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
  - name: "legacy_disabled"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 11
    service_id: 7849893
    profile: roman
    enabled: false
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="duplicate"):
            load_app_config(tmp_path)

    def test_two_pool_bookings_different_weekday_same_courts_ok(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  outdoor:
    service_id: 7849893
    court_ids: [10, 11]
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
  - name: "sat"
    weekday: saturday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2

    def test_same_pool_twice_same_slot_rejected(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  outdoor:
    service_id: 7849893
    court_ids: [10, 11]
bookings:
  - name: "first"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
  - name: "second"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="duplicate"):
            load_app_config(tmp_path)
