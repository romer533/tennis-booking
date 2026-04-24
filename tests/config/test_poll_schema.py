from __future__ import annotations

from datetime import time

import pytest
from pydantic import ValidationError

from tennis_booking.config.schema import (
    BookingRule,
    PollConfig,
    ResolvedBooking,
    Weekday,
)
from tests.config.test_schema import make_profile


def test_poll_config_valid_minimal() -> None:
    poll = PollConfig(interval_s=60, start_offset_days=2)
    assert poll.interval_s == 60
    assert poll.start_offset_days == 2


def test_poll_config_interval_floor_10() -> None:
    p = PollConfig(interval_s=10, start_offset_days=1)
    assert p.interval_s == 10


def test_poll_config_interval_below_floor_rejected() -> None:
    with pytest.raises(ValidationError):
        PollConfig(interval_s=9, start_offset_days=1)


def test_poll_config_interval_zero_rejected() -> None:
    with pytest.raises(ValidationError):
        PollConfig(interval_s=0, start_offset_days=1)


def test_poll_config_offset_min_1() -> None:
    p = PollConfig(interval_s=60, start_offset_days=1)
    assert p.start_offset_days == 1


def test_poll_config_offset_zero_rejected() -> None:
    with pytest.raises(ValidationError):
        PollConfig(interval_s=60, start_offset_days=0)


def test_poll_config_offset_max_30() -> None:
    p = PollConfig(interval_s=60, start_offset_days=30)
    assert p.start_offset_days == 30


def test_poll_config_offset_above_max_rejected() -> None:
    with pytest.raises(ValidationError):
        PollConfig(interval_s=60, start_offset_days=31)


def test_poll_config_extra_field_rejected() -> None:
    with pytest.raises(ValidationError):
        PollConfig(interval_s=60, start_offset_days=2, stop_after_slot=True)  # type: ignore[call-arg]


def test_poll_config_frozen() -> None:
    p = PollConfig(interval_s=60, start_offset_days=2)
    with pytest.raises(ValidationError):
        p.interval_s = 30  # type: ignore[misc]


def test_booking_rule_with_poll() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        poll=PollConfig(interval_s=60, start_offset_days=2),
    )
    assert rule.poll is not None
    assert rule.poll.interval_s == 60


def test_booking_rule_without_poll_default_none() -> None:
    rule = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
    )
    assert rule.poll is None


def test_booking_rule_poll_orthogonal_to_court_pool_xor() -> None:
    """poll should be allowed alongside either court_id+service_id OR court_pool."""
    # court_id path
    rule1 = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_id=5,
        service_id=7849893,
        profile="roman",
        poll=PollConfig(interval_s=60, start_offset_days=2),
    )
    assert rule1.poll is not None

    # court_pool path
    rule2 = BookingRule(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time="18:00",
        duration_minutes=60,
        court_pool="open-courts",
        profile="roman",
        poll=PollConfig(interval_s=60, start_offset_days=2),
    )
    assert rule2.poll is not None


def test_resolved_booking_poll_default_none() -> None:
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
    assert rb.poll is None


def test_resolved_booking_with_poll() -> None:
    rb = ResolvedBooking(
        name="x",
        weekday=Weekday.FRIDAY,
        slot_local_time=time(18, 0),
        duration_minutes=60,
        court_ids=(5,),
        service_id=7849893,
        profile=make_profile(),
        enabled=True,
        poll=PollConfig(interval_s=120, start_offset_days=3),
    )
    assert rb.poll is not None
    assert rb.poll.interval_s == 120
    assert rb.poll.start_offset_days == 3
