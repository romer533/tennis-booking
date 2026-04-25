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

MULTI_PROFILES = """\
profiles:
  roman:
    full_name: "R G"
    phone: "+77001234567"
  askar:
    full_name: "A K"
    phone: "+77002345678"
  daulet:
    full_name: "D N"
    phone: "+77003456789"
"""


def write_multi(tmp_path: Path, schedule: str) -> Path:
    (tmp_path / "profiles.yaml").write_text(MULTI_PROFILES, encoding="utf-8")
    (tmp_path / "schedule.yaml").write_text(schedule, encoding="utf-8")
    return tmp_path


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


class TestDedupAcrossProfiles:
    """Cross-profile dedup: different profiles legitimately compete for the
    same court+slot (whoever fires first wins). Same profile + same slot is
    still a real duplicate."""

    def test_two_bookings_same_pool_different_profiles_ok(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: [100, 101]
bookings:
  - name: "roman_mon20"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "askar_mon20"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: askar
"""
        write_multi(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
        names = {b.name for b in cfg.bookings}
        assert names == {"roman_mon20", "askar_mon20"}

    def test_three_bookings_same_pool_different_profiles_ok(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: [100, 101]
bookings:
  - name: "roman_mon20"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "askar_mon20"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: askar
  - name: "daulet_mon20"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: daulet
"""
        write_multi(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 3
        profiles_used = {b.profile.name for b in cfg.bookings}
        assert profiles_used == {"roman", "askar", "daulet"}

    def test_two_bookings_same_pool_same_profile_rejected(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: [100, 101]
bookings:
  - name: "roman_a"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "roman_b"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
"""
        write_multi(tmp_path, schedule)
        with pytest.raises(ConfigError, match="duplicate"):
            load_app_config(tmp_path)

    def test_pool_legacy_overlap_same_profile_rejected(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: [100, 101, 102]
bookings:
  - name: "pool_b"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "legacy_b"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_id: 101
    service_id: 7849893
    profile: roman
"""
        write_multi(tmp_path, schedule)
        with pytest.raises(ConfigError) as exc:
            load_app_config(tmp_path)
        msg = str(exc.value)
        assert "duplicate" in msg
        assert "101" in msg

    def test_pool_legacy_overlap_different_profile_ok(
        self, tmp_path: Path
    ) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: [100, 101, 102]
bookings:
  - name: "pool_b"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "legacy_b"
    weekday: monday
    slot_local_time: "20:00"
    duration_minutes: 60
    court_id: 101
    service_id: 7849893
    profile: askar
"""
        write_multi(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
