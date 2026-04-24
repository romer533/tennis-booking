import sys
from datetime import UTC, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from tennis_booking.scheduler.window import ATYRAU, next_open_window


def atyrau(year: int, month: int, day: int, hour: int = 0, minute: int = 0,
           second: int = 0, microsecond: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, second, microsecond, tzinfo=ATYRAU)


def utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


class TestBasic:
    def test_friday_evening_example(self) -> None:
        slot = atyrau(2026, 5, 15, 18, 0)
        assert next_open_window(slot) == utc(2026, 5, 12, 2, 0)

    def test_slot_exactly_at_open_local_time(self) -> None:
        slot = atyrau(2026, 5, 15, 7, 0)
        assert next_open_window(slot) == utc(2026, 5, 12, 2, 0)

    def test_slot_one_minute_before_open_local_time(self) -> None:
        slot = atyrau(2026, 5, 15, 6, 59)
        assert next_open_window(slot) == utc(2026, 5, 12, 2, 0)

    def test_slot_one_minute_after_open_local_time(self) -> None:
        slot = atyrau(2026, 5, 15, 7, 1)
        assert next_open_window(slot) == utc(2026, 5, 12, 2, 0)


class TestDayBoundaries:
    @pytest.mark.parametrize(
        ("hour", "minute", "second", "microsecond"),
        [
            (23, 30, 0, 0),
            (23, 59, 0, 0),
            (0, 0, 0, 0),
            (0, 1, 0, 0),
            (0, 0, 0, 1000),
        ],
    )
    def test_window_is_three_calendar_days_back_at_02_utc(
        self, hour: int, minute: int, second: int, microsecond: int
    ) -> None:
        slot = atyrau(2026, 5, 15, hour, minute, second, microsecond)
        result = next_open_window(slot)
        assert result == utc(2026, 5, 12, 2, 0)


class TestYearBoundary:
    def test_new_year_morning(self) -> None:
        slot = atyrau(2027, 1, 1, 0, 30)
        assert next_open_window(slot) == utc(2026, 12, 29, 2, 0)

    def test_new_year_evening(self) -> None:
        slot = atyrau(2027, 1, 1, 23, 30)
        assert next_open_window(slot) == utc(2026, 12, 29, 2, 0)


class TestMonthBoundary:
    def test_march_first(self) -> None:
        slot = atyrau(2026, 3, 1, 9, 0)
        assert next_open_window(slot) == utc(2026, 2, 26, 2, 0)

    def test_may_first(self) -> None:
        slot = atyrau(2026, 5, 1, 18, 0)
        assert next_open_window(slot) == utc(2026, 4, 28, 2, 0)


class TestLeapYear:
    def test_march_third_in_leap_year(self) -> None:
        slot = atyrau(2024, 3, 3, 9, 0)
        assert next_open_window(slot) == utc(2024, 2, 29, 2, 0)

    def test_march_first_in_leap_year(self) -> None:
        slot = atyrau(2024, 3, 1, 9, 0)
        assert next_open_window(slot) == utc(2024, 2, 27, 2, 0)

    def test_march_first_in_non_leap_year(self) -> None:
        slot = atyrau(2025, 3, 1, 9, 0)
        assert next_open_window(slot) == utc(2025, 2, 26, 2, 0)

    def test_leap_day_evening(self) -> None:
        slot = atyrau(2024, 2, 29, 18, 0)
        assert next_open_window(slot) == utc(2024, 2, 26, 2, 0)


class TestEndOfMonth:
    def test_last_day_of_march(self) -> None:
        slot = atyrau(2026, 3, 31, 18, 0)
        assert next_open_window(slot) == utc(2026, 3, 28, 2, 0)


WEEKDAY_DATES = [
    (2026, 5, 11),  # Mon
    (2026, 5, 12),  # Tue
    (2026, 5, 13),  # Wed
    (2026, 5, 14),  # Thu
    (2026, 5, 15),  # Fri
    (2026, 5, 16),  # Sat
    (2026, 5, 17),  # Sun
]


class TestAllWeekdays:
    @pytest.mark.parametrize(("year", "month", "day"), WEEKDAY_DATES)
    def test_each_weekday(self, year: int, month: int, day: int) -> None:
        slot = atyrau(year, month, day, 18, 0)
        result = next_open_window(slot)
        expected_local = atyrau(year, month, day, 7, 0) - timedelta(days=3)
        assert result == expected_local.astimezone(UTC)


MONTH_DATES = [
    (2026, 1, 15),
    (2026, 2, 15),
    (2026, 3, 15),
    (2026, 4, 15),
    (2026, 5, 15),
    (2026, 6, 15),
    (2026, 7, 15),
    (2026, 8, 15),
    (2026, 9, 15),
    (2026, 10, 15),
    (2026, 11, 15),
    (2026, 12, 15),
]


class TestAllMonths:
    @pytest.mark.parametrize(("year", "month", "day"), MONTH_DATES)
    def test_each_month(self, year: int, month: int, day: int) -> None:
        slot = atyrau(year, month, day, 18, 0)
        result = next_open_window(slot)
        expected_local = atyrau(year, month, day, 7, 0) - timedelta(days=3)
        assert result == expected_local.astimezone(UTC)


class TestReturnShape:
    def test_result_is_tz_aware_utc(self) -> None:
        result = next_open_window(atyrau(2026, 5, 15, 18, 0))
        assert result.tzinfo is UTC

    def test_result_seconds_minutes_microseconds_are_zero(self) -> None:
        result = next_open_window(atyrau(2026, 5, 15, 18, 0))
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_result_hour_is_two_utc(self) -> None:
        result = next_open_window(atyrau(2026, 5, 15, 18, 0))
        assert result.hour == 2

    def test_microseconds_in_input_are_ignored(self) -> None:
        slot = atyrau(2026, 5, 15, 18, 0, 0, 999999)
        result = next_open_window(slot)
        assert result.microsecond == 0
        assert result == utc(2026, 5, 12, 2, 0)


PROPERTY_DATES = [
    (2024, 1, 1, 0, 0),
    (2024, 1, 1, 23, 59),
    (2024, 2, 29, 12, 0),
    (2024, 6, 15, 7, 0),
    (2024, 12, 31, 23, 30),
    (2025, 1, 1, 0, 0),
    (2025, 3, 1, 6, 59),
    (2025, 7, 4, 14, 30),
    (2025, 10, 31, 23, 59),
    (2025, 12, 31, 12, 0),
    (2026, 1, 1, 0, 1),
    (2026, 2, 28, 18, 0),
    (2026, 3, 1, 0, 30),
    (2026, 4, 1, 12, 0),
    (2026, 5, 15, 18, 0),
    (2026, 6, 30, 23, 30),
    (2026, 7, 1, 0, 0),
    (2026, 8, 31, 7, 0),
    (2026, 9, 1, 7, 1),
    (2026, 10, 15, 9, 45),
    (2026, 11, 30, 22, 22),
    (2026, 12, 25, 13, 0),
    (2027, 1, 1, 0, 30),
    (2027, 1, 1, 23, 30),
    (2027, 2, 28, 8, 0),
    (2027, 5, 1, 18, 0),
    (2027, 7, 15, 7, 0),
    (2027, 9, 30, 23, 59),
    (2027, 11, 11, 11, 11),
    (2028, 2, 29, 18, 0),
]


class TestProperties:
    @pytest.mark.parametrize(("year", "month", "day", "hour", "minute"), PROPERTY_DATES)
    def test_window_is_seven_oclock_local(
        self, year: int, month: int, day: int, hour: int, minute: int
    ) -> None:
        slot = atyrau(year, month, day, hour, minute)
        result_local = next_open_window(slot).astimezone(ATYRAU)
        assert result_local.hour == 7
        assert result_local.minute == 0
        assert result_local.second == 0
        assert result_local.microsecond == 0

    @pytest.mark.parametrize(("year", "month", "day", "hour", "minute"), PROPERTY_DATES)
    def test_calendar_date_difference_in_atyrau_is_three_days(
        self, year: int, month: int, day: int, hour: int, minute: int
    ) -> None:
        slot = atyrau(year, month, day, hour, minute)
        result_local_date = next_open_window(slot).astimezone(ATYRAU).date()
        diff = (slot.date() - result_local_date).days
        assert diff == 3


class TestNegative:
    def test_naive_datetime_raises(self) -> None:
        naive = datetime(2026, 5, 15, 18, 0)
        with pytest.raises(ValueError, match="must be timezone-aware"):
            next_open_window(naive)

    def test_utc_zone_raises(self) -> None:
        slot = datetime(2026, 5, 15, 18, 0, tzinfo=UTC)
        with pytest.raises(ValueError, match="must be in Asia/Atyrau"):
            next_open_window(slot)

    def test_america_new_york_raises(self) -> None:
        slot = datetime(2026, 5, 15, 18, 0, tzinfo=ZoneInfo("America/New_York"))
        with pytest.raises(ValueError, match="must be in Asia/Atyrau"):
            next_open_window(slot)

    def test_europe_moscow_raises(self) -> None:
        slot = datetime(2026, 5, 15, 18, 0, tzinfo=ZoneInfo("Europe/Moscow"))
        with pytest.raises(ValueError, match="must be in Asia/Atyrau"):
            next_open_window(slot)

    def test_fixed_offset_plus_five_raises(self) -> None:
        slot = datetime(2026, 5, 15, 18, 0, tzinfo=timezone(timedelta(hours=5)))
        with pytest.raises(ValueError, match="must be in Asia/Atyrau"):
            next_open_window(slot)

    def test_none_raises(self) -> None:
        with pytest.raises((TypeError, ValueError, AttributeError)):
            next_open_window(None)  # type: ignore[arg-type]

    def test_string_raises(self) -> None:
        with pytest.raises((TypeError, ValueError, AttributeError)):
            next_open_window("2026-05-15T18:00:00+05:00")  # type: ignore[arg-type]


class TestPurity:
    def test_repeated_calls_return_same_value(self) -> None:
        slot = atyrau(2026, 5, 15, 18, 0)
        results = [next_open_window(slot) for _ in range(100)]
        assert all(r == results[0] for r in results)

    def test_does_not_mutate_input(self) -> None:
        slot = atyrau(2026, 5, 15, 18, 0)
        snapshot = (slot.year, slot.month, slot.day, slot.hour, slot.minute,
                    slot.second, slot.microsecond, slot.tzinfo)
        next_open_window(slot)
        after = (slot.year, slot.month, slot.day, slot.hour, slot.minute,
                 slot.second, slot.microsecond, slot.tzinfo)
        assert snapshot == after


class TestSystemTzImmunity:
    def test_system_tz_does_not_affect_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if sys.platform == "win32":
            pytest.skip("TZ env var is not honored by Windows libc / time module")
        monkeypatch.setenv("TZ", "America/New_York")
        slot = atyrau(2026, 5, 15, 18, 0)
        assert next_open_window(slot) == utc(2026, 5, 12, 2, 0)
