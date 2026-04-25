"""Тесты схемы GracePollingConfig + проводки через BookingRule/ResolvedBooking."""
from __future__ import annotations

from datetime import time

import pytest
from pydantic import ValidationError

from tennis_booking.config.schema import (
    BookingRule,
    GracePollingConfig,
    ResolvedBooking,
    Weekday,
)
from tests.config.test_schema import make_profile


def test_grace_polling_config_valid_minimal() -> None:
    g = GracePollingConfig(period_s=120, interval_s=10)
    assert g.period_s == 120
    assert g.interval_s == 10


def test_grace_polling_period_min_60_accepted() -> None:
    g = GracePollingConfig(period_s=60, interval_s=10)
    assert g.period_s == 60


def test_grace_polling_period_below_min_rejected() -> None:
    with pytest.raises(ValidationError):
        GracePollingConfig(period_s=59, interval_s=10)


def test_grace_polling_period_max_1800_accepted() -> None:
    g = GracePollingConfig(period_s=1800, interval_s=10)
    assert g.period_s == 1800


def test_grace_polling_period_above_max_rejected() -> None:
    with pytest.raises(ValidationError):
        GracePollingConfig(period_s=1801, interval_s=10)


def test_grace_polling_interval_min_10_accepted() -> None:
    g = GracePollingConfig(period_s=120, interval_s=10)
    assert g.interval_s == 10


def test_grace_polling_interval_below_min_rejected() -> None:
    with pytest.raises(ValidationError):
        GracePollingConfig(period_s=120, interval_s=9)


def test_grace_polling_interval_max_300_accepted() -> None:
    g = GracePollingConfig(period_s=600, interval_s=300)
    assert g.interval_s == 300


def test_grace_polling_interval_above_max_rejected() -> None:
    with pytest.raises(ValidationError):
        GracePollingConfig(period_s=600, interval_s=301)


def test_grace_polling_extra_field_rejected() -> None:
    with pytest.raises(ValidationError):
        GracePollingConfig(period_s=120, interval_s=10, foo=1)  # type: ignore[call-arg]


def test_grace_polling_frozen() -> None:
    g = GracePollingConfig(period_s=120, interval_s=10)
    with pytest.raises(ValidationError):
        g.period_s = 200  # type: ignore[misc]


def test_booking_rule_grace_polling_default_none() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
    )
    assert rule.grace_polling is None


def test_booking_rule_with_grace_polling() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        grace_polling=GracePollingConfig(period_s=120, interval_s=10),
    )
    assert rule.grace_polling is not None
    assert rule.grace_polling.period_s == 120


def test_resolved_booking_grace_polling_default_none() -> None:
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
    assert rb.grace_polling is None


def test_resolved_booking_with_grace_polling() -> None:
    rb = ResolvedBooking(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time=time(18, 0),
        duration_minutes=60,
        court_ids=(5,),
        service_id=7849893,
        profile=make_profile(),
        enabled=True,
        grace_polling=GracePollingConfig(period_s=300, interval_s=15),
    )
    assert rb.grace_polling is not None
    assert rb.grace_polling.period_s == 300
    assert rb.grace_polling.interval_s == 15


def test_booking_rule_grace_polling_orthogonal_to_poll() -> None:
    """grace_polling и poll — независимые опции, могут сосуществовать."""
    from tennis_booking.config.schema import PollConfig

    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        poll=PollConfig(interval_s=60, start_offset_days=2),
        grace_polling=GracePollingConfig(period_s=120, interval_s=10),
    )
    assert rule.poll is not None
    assert rule.grace_polling is not None
