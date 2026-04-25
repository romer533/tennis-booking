"""Tests for min_lead_time_hours propagation through SchedulerLoop.

Verifies:
  - SchedulerLoop validates the bound at construction.
  - App default is propagated into AttemptConfig when booking has no override.
  - Per-booking override wins over app default.
"""
from __future__ import annotations

from datetime import time
from types import MappingProxyType

import pytest

from tennis_booking.config.schema import (
    AppConfig,
    Profile,
    ResolvedBooking,
    Weekday,
)
from tennis_booking.scheduler.loop import (
    DEFAULT_MIN_LEAD_TIME_HOURS,
    ScheduledAttempt,
    SchedulerLoop,
)

from .conftest import (
    SERVICE_ID,
    STAFF_ID,
    as_altegio_client,
    as_clock,
)


def _profile() -> Profile:
    return Profile(
        name="roman",
        full_name="Roman G",
        phone="77001234567",
        email="r@x.com",
    )


def _booking(
    name: str = "fri",
    min_lead_time_hours: float | None = None,
) -> ResolvedBooking:
    return ResolvedBooking(
        name=name,
        weekday=Weekday.FRIDAY,
        slot_local_time=time(18, 0),
        duration_minutes=60,
        court_ids=(STAFF_ID,),
        service_id=SERVICE_ID,
        profile=_profile(),
        enabled=True,
        min_lead_time_hours=min_lead_time_hours,
    )


def _config(*bookings: ResolvedBooking) -> AppConfig:
    return AppConfig(
        bookings=tuple(bookings),
        profiles=MappingProxyType({"roman": _profile()}),
        court_pools=MappingProxyType({}),
    )


def test_scheduler_loop_default_min_lead_zero(
    make_clock,
    fake_client,
) -> None:
    loop = SchedulerLoop(
        config=_config(),
        altegio_client=as_altegio_client(fake_client([])),
        clock=as_clock(make_clock()),
    )
    assert loop._min_lead_time_hours == DEFAULT_MIN_LEAD_TIME_HOURS == 0.0


def test_scheduler_loop_negative_min_lead_rejected(
    make_clock,
    fake_client,
) -> None:
    with pytest.raises(ValueError, match="min_lead_time_hours"):
        SchedulerLoop(
            config=_config(),
            altegio_client=as_altegio_client(fake_client([])),
            clock=as_clock(make_clock()),
            min_lead_time_hours=-1.0,
        )


def test_scheduler_loop_above_max_min_lead_rejected(
    make_clock,
    fake_client,
) -> None:
    with pytest.raises(ValueError, match="min_lead_time_hours"):
        SchedulerLoop(
            config=_config(),
            altegio_client=as_altegio_client(fake_client([])),
            clock=as_clock(make_clock()),
            min_lead_time_hours=200.0,
        )


def test_build_attempt_config_uses_app_default(
    make_clock,
    fake_client,
) -> None:
    loop = SchedulerLoop(
        config=_config(),
        altegio_client=as_altegio_client(fake_client([])),
        clock=as_clock(make_clock()),
        min_lead_time_hours=2.0,
    )
    booking = _booking(min_lead_time_hours=None)
    from datetime import datetime

    from tennis_booking.common.tz import ALMATY

    sa = ScheduledAttempt(
        booking=booking,
        slot_dt_local=datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY),
        window_open_utc=datetime(2026, 4, 23, 2, 0, tzinfo=__import__("datetime").timezone.utc),
    )
    cfg = loop._build_attempt_config(sa)
    assert cfg.min_lead_time_hours == 2.0


def test_build_attempt_config_per_booking_override_wins(
    make_clock,
    fake_client,
) -> None:
    loop = SchedulerLoop(
        config=_config(),
        altegio_client=as_altegio_client(fake_client([])),
        clock=as_clock(make_clock()),
        min_lead_time_hours=2.0,
    )
    booking = _booking(min_lead_time_hours=4.0)
    from datetime import datetime

    from tennis_booking.common.tz import ALMATY

    sa = ScheduledAttempt(
        booking=booking,
        slot_dt_local=datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY),
        window_open_utc=datetime(2026, 4, 23, 2, 0, tzinfo=__import__("datetime").timezone.utc),
    )
    cfg = loop._build_attempt_config(sa)
    assert cfg.min_lead_time_hours == 4.0


def test_build_attempt_config_per_booking_zero_override(
    make_clock,
    fake_client,
) -> None:
    """Per-booking explicit 0.0 disables the guard for this booking even when
    app default is non-zero (e.g. one booking is for a slot far in the future
    where guard is irrelevant, or for testing).
    """
    loop = SchedulerLoop(
        config=_config(),
        altegio_client=as_altegio_client(fake_client([])),
        clock=as_clock(make_clock()),
        min_lead_time_hours=2.0,
    )
    booking = _booking(min_lead_time_hours=0.0)
    from datetime import datetime

    from tennis_booking.common.tz import ALMATY

    sa = ScheduledAttempt(
        booking=booking,
        slot_dt_local=datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY),
        window_open_utc=datetime(2026, 4, 23, 2, 0, tzinfo=__import__("datetime").timezone.utc),
    )
    cfg = loop._build_attempt_config(sa)
    assert cfg.min_lead_time_hours == 0.0
