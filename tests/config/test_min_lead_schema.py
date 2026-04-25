"""Tests for `min_lead_time_hours` field on BookingRule and ResolvedBooking."""
from __future__ import annotations

from datetime import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from tennis_booking.config import ConfigError, load_app_config
from tennis_booking.config.schema import (
    BookingRule,
    ResolvedBooking,
    Weekday,
)
from tests.config.test_schema import make_profile

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


# ---- BookingRule field validation -----------------------------------------


def test_booking_rule_min_lead_default_none() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
    )
    assert rule.min_lead_time_hours is None


def test_booking_rule_min_lead_valid_float() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        min_lead_time_hours=1.5,
    )
    assert rule.min_lead_time_hours == 1.5


def test_booking_rule_min_lead_zero_accepted() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        min_lead_time_hours=0.0,
    )
    assert rule.min_lead_time_hours == 0.0


def test_booking_rule_min_lead_negative_rejected() -> None:
    with pytest.raises(ValidationError):
        BookingRule(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time="18:00",
            duration_minutes=60,
            court_id=5,
            service_id=7849893,
            profile="roman",
            min_lead_time_hours=-1,
        )


def test_booking_rule_min_lead_above_max_rejected() -> None:
    with pytest.raises(ValidationError):
        BookingRule(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time="18:00",
            duration_minutes=60,
            court_id=5,
            service_id=7849893,
            profile="roman",
            min_lead_time_hours=200,
        )


def test_booking_rule_min_lead_max_boundary_accepted() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        min_lead_time_hours=168.0,
    )
    assert rule.min_lead_time_hours == 168.0


# ---- ResolvedBooking field validation -------------------------------------


def test_resolved_booking_min_lead_default_none() -> None:
    rb = ResolvedBooking(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time=time(18, 0),
        duration_minutes=60,
        court_ids=(5,),
        service_id=7849893,
        profile=make_profile(),
        enabled=True,
    )
    assert rb.min_lead_time_hours is None


def test_resolved_booking_min_lead_pass_through() -> None:
    rb = ResolvedBooking(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time=time(18, 0),
        duration_minutes=60,
        court_ids=(5,),
        service_id=7849893,
        profile=make_profile(),
        enabled=True,
        min_lead_time_hours=2.5,
    )
    assert rb.min_lead_time_hours == 2.5


# ---- Loader pass-through --------------------------------------------------


def test_loader_propagates_min_lead_to_resolved(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    min_lead_time_hours: 4.0
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    cfg = load_app_config(tmp_path)
    assert cfg.bookings[0].min_lead_time_hours == 4.0


def test_loader_min_lead_unset_remains_none(tmp_path: Path) -> None:
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
    assert cfg.bookings[0].min_lead_time_hours is None


def test_loader_min_lead_negative_rejected(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    min_lead_time_hours: -1
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="min_lead_time_hours"):
        load_app_config(tmp_path)


def test_loader_min_lead_above_max_rejected(tmp_path: Path) -> None:
    schedule = """\
bookings:
  - name: "x"
    weekday: friday
    slot_local_time: "18:00"
    duration_minutes: 60
    court_id: 5
    service_id: 7849893
    profile: roman
    min_lead_time_hours: 200
"""
    _write(tmp_path, GOOD_PROFILES, schedule)
    with pytest.raises(ConfigError, match="min_lead_time_hours"):
        load_app_config(tmp_path)
