from __future__ import annotations

import logging
from pathlib import Path

import pytest

from tennis_booking.config import (
    AppConfig,
    ConfigError,
    Weekday,
    load_app_config,
    load_profiles,
    load_schedule,
)

GOOD_PROFILES = """\
profiles:
  roman:
    full_name: "Иванов Иван Иванович"
    phone: "+77001234567"
    email: "test@example.com"
"""

GOOD_SCHEDULE = """\
bookings:
  - name: "Пятница вечер"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    enabled: true
"""


def write_config(tmp_path: Path, profiles: str, schedule: str) -> Path:
    (tmp_path / "profiles.yaml").write_text(profiles, encoding="utf-8")
    (tmp_path / "schedule.yaml").write_text(schedule, encoding="utf-8")
    return tmp_path


class TestHappyPath:
    def test_load_app_config_basic(self, tmp_path: Path) -> None:
        write_config(tmp_path, GOOD_PROFILES, GOOD_SCHEDULE)
        cfg = load_app_config(tmp_path)
        assert isinstance(cfg, AppConfig)
        assert len(cfg.bookings) == 1
        assert "roman" in cfg.profiles
        rb = cfg.bookings[0]
        assert rb.name == "Пятница вечер"
        assert rb.weekday == Weekday.FRIDAY
        assert rb.court_id == 5
        assert rb.service_id == 7849893
        assert rb.profile is cfg.profiles["roman"]

    def test_load_example_config_files(self, tmp_path: Path) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        cfg_dir = repo_root / "config"
        (tmp_path / "profiles.yaml").write_text(
            (cfg_dir / "profiles.example.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (tmp_path / "schedule.yaml").write_text(
            (cfg_dir / "schedule.example.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        # example schedule has court_id: 0 (placeholder) → must be rejected
        with pytest.raises(ConfigError, match="court_id"):
            load_app_config(tmp_path)

    def test_example_schedule_with_court_id_filled_loads(self, tmp_path: Path) -> None:
        # After Phase 0 the user replaces court_id: 0 → real id. Example service_id
        # must already be valid so nothing else blocks loading.
        repo_root = Path(__file__).resolve().parents[2]
        cfg_dir = repo_root / "config"
        (tmp_path / "profiles.yaml").write_text(
            (cfg_dir / "profiles.example.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        schedule_text = (cfg_dir / "schedule.example.yaml").read_text(encoding="utf-8")
        # Replace only the first court_id: 0 (enabled booking); disabled one still errors
        # on its own court_id: 0, so replace both.
        schedule_text = schedule_text.replace("court_id: 0", "court_id: 1521566")
        (tmp_path / "schedule.yaml").write_text(schedule_text, encoding="utf-8")
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
        for rb in cfg.bookings:
            assert rb.service_id == 7849893

    def test_two_profiles_two_bookings(self, tmp_path: Path) -> None:
        profiles = """\
profiles:
  roman:
    full_name: "Roman G"
    phone: "+77001234567"
    email: "r@x.com"
  alex:
    full_name: "Alex P"
    phone: "+77007654321"
"""
        schedule = """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
  - name: "sun"
    weekday: sunday
    slot_local_time: "09:00"
    duration_minutes: 90
    court_id: 6
    service_id: 7849893
    profile: alex
"""
        write_config(tmp_path, profiles, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
        assert len(cfg.profiles) == 2
        assert cfg.bookings[0].profile.name == "roman"
        assert cfg.bookings[1].profile.name == "alex"
        assert cfg.bookings[1].profile.email is None

    def test_disabled_booking_present_in_result(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
  - name: "sun"
    weekday: sunday
    slot_local_time: "09:00"
    duration_minutes: 60
    court_id: 6
    service_id: 7849893
    profile: roman
    enabled: false
"""
        write_config(tmp_path, GOOD_PROFILES, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2
        assert cfg.bookings[0].enabled is True
        assert cfg.bookings[1].enabled is False


class TestFileMissing:
    def test_schedule_missing(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        with pytest.raises(ConfigError, match="schedule.yaml not found"):
            load_app_config(tmp_path)

    def test_schedule_missing_hint(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        with pytest.raises(ConfigError, match="schedule.example.yaml"):
            load_app_config(tmp_path)

    def test_profiles_missing(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        with pytest.raises(ConfigError, match="profiles.yaml not found"):
            load_app_config(tmp_path)

    def test_profiles_missing_hint(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        with pytest.raises(ConfigError, match="profiles.example.yaml"):
            load_app_config(tmp_path)

    def test_config_dir_missing(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="config directory not found"):
            load_app_config(tmp_path / "nonexistent")

    def test_config_path_is_file(self, tmp_path: Path) -> None:
        f = tmp_path / "x.txt"
        f.write_text("hi", encoding="utf-8")
        with pytest.raises(ConfigError, match="not a directory"):
            load_app_config(f)


class TestEmptyFiles:
    def test_empty_schedule_warns_zero_bookings(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text("", encoding="utf-8")
        with caplog.at_level(logging.WARNING):
            cfg = load_app_config(tmp_path)
        assert cfg.bookings == ()
        assert any("0 bookings" in r.message for r in caplog.records)

    def test_schedule_with_null_bookings_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text("bookings: null\n", encoding="utf-8")
        with caplog.at_level(logging.WARNING):
            cfg = load_app_config(tmp_path)
        assert cfg.bookings == ()
        assert any("0 bookings" in r.message for r in caplog.records)

    def test_schedule_with_empty_bookings_list_warns(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text("bookings: []\n", encoding="utf-8")
        with caplog.at_level(logging.WARNING):
            cfg = load_app_config(tmp_path)
        assert cfg.bookings == ()
        assert any("0 bookings" in r.message for r in caplog.records)

    def test_empty_profiles_raises(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text("", encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        with pytest.raises(ConfigError, match="at least one profile required"):
            load_app_config(tmp_path)

    def test_profiles_null_raises(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text("profiles: null\n", encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        with pytest.raises(ConfigError, match="at least one profile"):
            load_app_config(tmp_path)

    def test_profiles_empty_dict_raises(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text("profiles: {}\n", encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        with pytest.raises(ConfigError, match="0 profiles"):
            load_app_config(tmp_path)


class TestBrokenYaml:
    def test_broken_schedule(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(
            "bookings:\n  - name: x\n  bad indent\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="invalid YAML in schedule.yaml"):
            load_app_config(tmp_path)

    def test_broken_yaml_includes_line_info(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(
            "bookings:\n  - name: [unclosed\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="line"):
            load_app_config(tmp_path)

    def test_broken_profiles(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_text(
            "profiles:\n  roman:\n    name: [unclosed\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="invalid YAML in profiles.yaml"):
            load_app_config(tmp_path)


class TestRootShape:
    def test_schedule_root_is_list(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text("- a\n- b\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="root must be a mapping"):
            load_app_config(tmp_path)

    def test_profiles_root_is_list(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_text("- a\n- b\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="root must be a mapping"):
            load_app_config(tmp_path)

    def test_schedule_extra_top_level_key(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(
            "bookings: []\nextra: 1\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="unexpected top-level keys"):
            load_app_config(tmp_path)

    def test_profiles_extra_top_level_key(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_text(
            GOOD_PROFILES + "extra: 1\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="unexpected top-level keys"):
            load_app_config(tmp_path)

    def test_profiles_must_be_mapping(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_text("profiles:\n  - a\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="must be a mapping"):
            load_app_config(tmp_path)

    def test_bookings_must_be_list(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(
            "bookings:\n  foo: bar\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="must be a list"):
            load_app_config(tmp_path)

    def test_booking_item_not_mapping(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(
            "bookings:\n  - just a string\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="must be a mapping"):
            load_app_config(tmp_path)

    def test_profile_value_not_mapping(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_text(
            "profiles:\n  roman: oops\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="must be a mapping"):
            load_app_config(tmp_path)

    def test_profile_name_not_string(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_text(
            "profiles:\n  123:\n    full_name: x\n    phone: '+1'\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError, match="must be a string"):
            load_app_config(tmp_path)

    def test_yaml_error_without_mark(self, tmp_path: Path) -> None:
        # тег без определения сериализатора — YAMLError без MarkedYAMLError
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_text(
            "bookings:\n  - !!python/object:nonexistent.Class {}\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigError, match="invalid YAML"):
            load_app_config(tmp_path)


class TestValidationWrapping:
    def test_invalid_court_id_zero(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("court_id: 5", "court_id: 0")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="court_id"):
            load_app_config(tmp_path)

    def test_invalid_service_id_zero(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("service_id: 7849893", "service_id: 0")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="service_id"):
            load_app_config(tmp_path)

    def test_missing_service_id(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("    service_id: 7849893\n", "")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="service_id"):
            load_app_config(tmp_path)

    def test_invalid_duration(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("duration_minutes: 60", "duration_minutes: 999")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="duration_minutes"):
            load_app_config(tmp_path)

    def test_invalid_weekday(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("weekday: friday", "weekday: funday")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="weekday"):
            load_app_config(tmp_path)

    def test_yaml_native_time_int_rejected(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace('slot_local_time: "18:00"', "slot_local_time: 18:00")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="slot_local_time"):
            load_app_config(tmp_path)

    def test_slot_time_invalid_string(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace('slot_local_time: "18:00"', 'slot_local_time: "7:00"')
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="slot_local_time"):
            load_app_config(tmp_path)

    def test_extra_field_in_booking(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE + "    extra: 1\n"
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="extra"):
            load_app_config(tmp_path)

    def test_extra_field_in_profile(self, tmp_path: Path) -> None:
        bad = GOOD_PROFILES + "    extra: foo\n"
        write_config(tmp_path, bad, GOOD_SCHEDULE)
        with pytest.raises(ConfigError):
            load_app_config(tmp_path)

    def test_empty_full_name(self, tmp_path: Path) -> None:
        bad = GOOD_PROFILES.replace('"Иванов Иван Иванович"', '""')
        write_config(tmp_path, bad, GOOD_SCHEDULE)
        with pytest.raises(ConfigError, match="full_name"):
            load_app_config(tmp_path)

    def test_empty_phone(self, tmp_path: Path) -> None:
        bad = GOOD_PROFILES.replace('"+77001234567"', '""')
        write_config(tmp_path, bad, GOOD_SCHEDULE)
        with pytest.raises(ConfigError, match="phone"):
            load_app_config(tmp_path)

    def test_invalid_profile_name(self, tmp_path: Path) -> None:
        bad = """\
profiles:
  Roman:
    full_name: "X"
    phone: "+1"
"""
        write_config(tmp_path, bad, GOOD_SCHEDULE.replace("profile: roman", "profile: roman"))
        with pytest.raises(ConfigError, match="name"):
            load_app_config(tmp_path)

    def test_email_empty_string_normalized(self, tmp_path: Path) -> None:
        good = GOOD_PROFILES.replace('"test@example.com"', '""')
        write_config(tmp_path, good, GOOD_SCHEDULE)
        cfg = load_app_config(tmp_path)
        assert cfg.profiles["roman"].email is None

    def test_email_omitted(self, tmp_path: Path) -> None:
        profiles = """\
profiles:
  roman:
    full_name: "X"
    phone: "+1"
"""
        write_config(tmp_path, profiles, GOOD_SCHEDULE)
        cfg = load_app_config(tmp_path)
        assert cfg.profiles["roman"].email is None


class TestCrossValidation:
    def test_unknown_profile_ref(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("profile: roman", "profile: nobody")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="unknown profile"):
            load_app_config(tmp_path)

    def test_unknown_profile_ref_lists_known(self, tmp_path: Path) -> None:
        bad = GOOD_SCHEDULE.replace("profile: roman", "profile: nobody")
        write_config(tmp_path, GOOD_PROFILES, bad)
        with pytest.raises(ConfigError, match="roman"):
            load_app_config(tmp_path)

    def test_duplicate_slot(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "first"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
  - name: "second"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write_config(tmp_path, GOOD_PROFILES, schedule)
        with pytest.raises(ConfigError, match="duplicate"):
            load_app_config(tmp_path)

    def test_duplicate_slot_includes_both_names(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "alpha"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
  - name: "beta"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write_config(tmp_path, GOOD_PROFILES, schedule)
        with pytest.raises(ConfigError) as exc:
            load_app_config(tmp_path)
        assert "alpha" in str(exc.value)
        assert "beta" in str(exc.value)

    def test_duplicate_includes_disabled(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "first"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    enabled: true
  - name: "second"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    enabled: false
"""
        write_config(tmp_path, GOOD_PROFILES, schedule)
        with pytest.raises(ConfigError, match="duplicate"):
            load_app_config(tmp_path)

    def test_different_courts_same_time_ok(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "court5"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
  - name: "court6"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 6
    service_id: 7849893
    profile: roman
"""
        write_config(tmp_path, GOOD_PROFILES, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2

    def test_different_weekdays_same_time_court_ok(self, tmp_path: Path) -> None:
        schedule = """\
bookings:
  - name: "fri"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
  - name: "sat"
    weekday: saturday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
"""
        write_config(tmp_path, GOOD_PROFILES, schedule)
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 2

    def test_unreferenced_profile_warns_not_errors(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        profiles = """\
profiles:
  roman:
    full_name: "R"
    phone: "+1"
  ghost:
    full_name: "G"
    phone: "+2"
"""
        write_config(tmp_path, profiles, GOOD_SCHEDULE)
        with caplog.at_level(logging.WARNING):
            cfg = load_app_config(tmp_path)
        assert "ghost" in cfg.profiles
        assert any("ghost" in r.message for r in caplog.records)


class TestFileEncoding:
    def test_bom_in_schedule(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        (tmp_path / "schedule.yaml").write_bytes(
            b"\xef\xbb\xbf" + GOOD_SCHEDULE.encode("utf-8")
        )
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 1

    def test_bom_in_profiles(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        (tmp_path / "profiles.yaml").write_bytes(
            b"\xef\xbb\xbf" + GOOD_PROFILES.encode("utf-8")
        )
        cfg = load_app_config(tmp_path)
        assert "roman" in cfg.profiles

    def test_crlf_line_endings(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_bytes(
            GOOD_PROFILES.replace("\n", "\r\n").encode("utf-8")
        )
        (tmp_path / "schedule.yaml").write_bytes(
            GOOD_SCHEDULE.replace("\n", "\r\n").encode("utf-8")
        )
        cfg = load_app_config(tmp_path)
        assert len(cfg.bookings) == 1


class TestImmutability:
    def test_app_config_frozen(self, tmp_path: Path) -> None:
        write_config(tmp_path, GOOD_PROFILES, GOOD_SCHEDULE)
        cfg = load_app_config(tmp_path)
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            cfg.bookings = ()  # type: ignore[misc]

    def test_profiles_mapping_immutable(self, tmp_path: Path) -> None:
        write_config(tmp_path, GOOD_PROFILES, GOOD_SCHEDULE)
        cfg = load_app_config(tmp_path)
        with pytest.raises(TypeError):
            cfg.profiles["new"] = cfg.profiles["roman"]  # type: ignore[index]


class TestLowLevelHelpers:
    def test_load_profiles_alone(self, tmp_path: Path) -> None:
        (tmp_path / "profiles.yaml").write_text(GOOD_PROFILES, encoding="utf-8")
        result = load_profiles(tmp_path / "profiles.yaml")
        assert "roman" in result
        assert result["roman"].name == "roman"

    def test_load_schedule_alone(self, tmp_path: Path) -> None:
        (tmp_path / "schedule.yaml").write_text(GOOD_SCHEDULE, encoding="utf-8")
        result = load_schedule(tmp_path / "schedule.yaml")
        assert len(result) == 1
        assert result[0].profile == "roman"
        assert result[0].service_id == 7849893
