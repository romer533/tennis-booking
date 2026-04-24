"""Loader tests for court_pools section + pool-based bookings + legacy compat."""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

from tennis_booking.config import ConfigError, load_app_config

GOOD_PROFILES = """\
profiles:
  roman:
    full_name: "R G"
    phone: "+77001234567"
"""

POOLS_SECTION = """\
court_pools:
  indoor:
    service_id: 7790744
    court_ids: [1513587, 1521553, 1521555]
  outdoor:
    service_id: 7849893
    court_ids: [1521564, 1521565, 1521566, 1521567]
"""


def write(tmp_path: Path, schedule: str, profiles: str = GOOD_PROFILES) -> Path:
    (tmp_path / "profiles.yaml").write_text(profiles, encoding="utf-8")
    (tmp_path / "schedule.yaml").write_text(schedule, encoding="utf-8")
    return tmp_path


# ---------- pool-based bookings ---------------------------------------------


class TestPoolBasedBooking:
    def test_pool_booking_expands_to_court_ids(self, tmp_path: Path) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "Sun indoor"
    weekday: sunday
    slot_local_time: "23:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 1
        rb = cfg.bookings[0]
        assert rb.court_ids == (1513587, 1521553, 1521555)
        assert rb.service_id == 7790744
        assert rb.pool_name == "indoor"

    def test_legacy_booking_alongside_pool_booking(self, tmp_path: Path) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "Sun pool"
    weekday: sunday
    slot_local_time: "23:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "Fri court 5"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 1521564
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
        pool_b = cfg.bookings[0]
        legacy_b = cfg.bookings[1]
        assert pool_b.pool_name == "indoor"
        assert pool_b.court_ids == (1513587, 1521553, 1521555)
        assert legacy_b.pool_name is None
        assert legacy_b.court_ids == (1521564,)
        assert legacy_b.service_id == 7849893

    def test_pool_with_one_court_works(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  solo:
    service_id: 7849893
    court_ids: [1234]
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: solo
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert cfg.bookings[0].court_ids == (1234,)
        assert cfg.bookings[0].service_id == 7849893

    def test_two_pool_bookings_different_pools(self, tmp_path: Path) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "in"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
  - name: "out"
    weekday: friday
    slot_local_time: "19:00"
    duration_minutes: 60
    court_pool: outdoor
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
        assert cfg.bookings[0].pool_name == "indoor"
        assert cfg.bookings[1].pool_name == "outdoor"


# ---------- pool errors ------------------------------------------------------


class TestPoolErrors:
    def test_unknown_pool_referenced(self, tmp_path: Path) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "x"
    weekday: sunday
    slot_local_time: "23:00"
    duration_minutes: 60
    court_pool: nonexistent
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="unknown pool 'nonexistent'"):
            load_app_config(tmp_path)

    def test_unknown_pool_lists_known(self, tmp_path: Path) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "x"
    weekday: sunday
    slot_local_time: "23:00"
    duration_minutes: 60
    court_pool: nope
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError) as exc:
            load_app_config(tmp_path)
        msg = str(exc.value)
        assert "indoor" in msg and "outdoor" in msg

    def test_pool_and_court_id_both_specified(self, tmp_path: Path) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "x"
    weekday: sunday
    slot_local_time: "23:00"
    duration_minutes: 60
    court_pool: indoor
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="either court_pool"):
            load_app_config(tmp_path)

    def test_neither_pool_nor_court_specified(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "x"
    weekday: sunday
    slot_local_time: "23:00"
    duration_minutes: 60
    profile: roman
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="court_pool"):
            load_app_config(tmp_path)

    def test_invalid_pool_name_in_section(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  Indoor:
    service_id: 1
    court_ids: [1, 2]
bookings: []
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="court_pool name"):
            load_app_config(tmp_path)

    def test_pool_not_a_mapping(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  - bad
bookings: []
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="court_pools"):
            load_app_config(tmp_path)

    def test_pool_value_not_a_mapping(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  indoor: oops
bookings: []
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="must be a mapping"):
            load_app_config(tmp_path)

    def test_pool_empty_court_ids(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: []
bookings: []
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="court_ids"):
            load_app_config(tmp_path)


# ---------- backward compat: no court_pools section -------------------------


class TestLegacyCompat:
    def test_no_court_pools_section_works(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.court_pools) == 0
        assert cfg.bookings[0].court_ids == (5,)
        assert cfg.bookings[0].pool_name is None

    def test_empty_court_pools_section(self, tmp_path: Path) -> None:
        schedule = """\
court_pools: {}
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.court_pools) == 0

    def test_null_court_pools_section(self, tmp_path: Path) -> None:
        schedule = """\
court_pools: null
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write(tmp_path, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.court_pools) == 0


# ---------- unused pool warning ---------------------------------------------


class TestUnusedPool:
    def test_unused_pool_warns_not_errors(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        schedule = POOLS_SECTION + """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
"""
        write(tmp_path, schedule)
        with caplog.at_level(logging.WARNING):
            cfg = load_app_config(tmp_path)
        # outdoor is defined but not referenced
        assert "outdoor" in cfg.court_pools
        assert any("outdoor" in r.message and "court_pool" in r.message for r in caplog.records)

    def test_used_pool_does_not_warn(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 7849893
    court_ids: [1, 2]
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_pool: indoor
    profile: roman
"""
        write(tmp_path, schedule)
        with caplog.at_level(logging.WARNING):
            load_app_config(tmp_path)
        for r in caplog.records:
            assert not ("indoor" in r.message and "court_pool" in r.message)


# ---------- additional coverage edge cases ----------------------------------


class TestLoaderEdgeCoverage:
    def test_court_pool_section_root_not_mapping(self, tmp_path: Path) -> None:
        # schedule.yaml root not mapping → load_court_pools must raise.
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text("- a\n- b\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="root must be a mapping"):
            load_app_config(tmp_path)

    def test_court_pool_name_is_int(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  123:
    service_id: 1
    court_ids: [1]
bookings: []
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError, match="must be a string"):
            load_app_config(tmp_path)

    def test_court_pool_extra_field_in_pool_data(self, tmp_path: Path) -> None:
        schedule = """\
court_pools:
  indoor:
    service_id: 1
    court_ids: [1]
    extra: bad
bookings: []
"""
        write(tmp_path, schedule)
        with pytest.raises(ConfigError):
            load_app_config(tmp_path)

    def test_read_oserror_wrapped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If Path.read_text raises OSError (e.g. permissions), loader wraps it.
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text("bookings: []\n", encoding="utf-8")

        original = Path.read_text

        def fail_read(self: Path, *args: object, **kwargs: object) -> str:
            if self.name == "schedule.yaml":
                raise OSError("simulated EACCES")
            return original(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(Path, "read_text", fail_read)
        with pytest.raises(ConfigError, match="failed to read"):
            load_app_config(tmp_path)
