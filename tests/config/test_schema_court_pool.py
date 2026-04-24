"""Tests for CourtPool model and BookingRule XOR (court_pool vs court_id+service_id)."""
from __future__ import annotations

from datetime import time

import pytest
from pydantic import ValidationError

from tennis_booking.config.schema import (
    BookingRule,
    CourtPool,
    ResolvedBooking,
    Weekday,
)
from tests.config.test_schema import make_profile

# ---------- CourtPool model ---------------------------------------------------


class TestCourtPoolValid:
    def test_minimal_pool(self) -> None:
        p = CourtPool(service_id=7849893, court_ids=(100,))
        assert p.service_id == 7849893
        assert p.court_ids == (100,)

    def test_multi_court_pool(self) -> None:
        p = CourtPool(service_id=7849893, court_ids=(100, 200, 300))
        assert p.court_ids == (100, 200, 300)

    def test_list_coerced_to_tuple(self) -> None:
        p = CourtPool(service_id=1, court_ids=[1, 2, 3])  # type: ignore[arg-type]
        assert isinstance(p.court_ids, tuple)
        assert p.court_ids == (1, 2, 3)

    def test_frozen(self) -> None:
        p = CourtPool(service_id=1, court_ids=(1,))
        with pytest.raises(ValidationError):
            p.service_id = 2  # type: ignore[misc]


class TestCourtPoolInvalid:
    def test_empty_court_ids_rejected(self) -> None:
        with pytest.raises(ValidationError, match="court_ids"):
            CourtPool(service_id=1, court_ids=())

    def test_zero_court_id_rejected(self) -> None:
        with pytest.raises(ValidationError, match="court_ids"):
            CourtPool(service_id=1, court_ids=(0,))

    def test_negative_court_id_rejected(self) -> None:
        with pytest.raises(ValidationError, match="court_ids"):
            CourtPool(service_id=1, court_ids=(-1,))

    def test_duplicate_court_ids_rejected(self) -> None:
        with pytest.raises(ValidationError, match="unique"):
            CourtPool(service_id=1, court_ids=(1, 2, 1))

    def test_zero_service_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CourtPool(service_id=0, court_ids=(1,))

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            CourtPool(  # type: ignore[call-arg]
                service_id=1, court_ids=(1,), extra="boo"
            )

    def test_non_int_court_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CourtPool(service_id=1, court_ids=("abc",))  # type: ignore[arg-type]


# ---------- BookingRule XOR validation ---------------------------------------


def _make_rule(**overrides: object) -> BookingRule:
    base: dict[str, object] = {
        "name": "x",
        "weekday": Weekday.FRIDAY,
        "slot_local_time": "18:00",
        "duration_minutes": 60,
        "profile": "roman",
    }
    base.update(overrides)
    return BookingRule(**base)  # type: ignore[arg-type]


class TestBookingRuleXOR:
    def test_pool_only_valid(self) -> None:
        b = _make_rule(court_pool="indoor")
        assert b.court_pool == "indoor"
        assert b.court_id is None
        assert b.service_id is None

    def test_legacy_court_and_service_valid(self) -> None:
        b = _make_rule(court_id=5, service_id=7849893)
        assert b.court_id == 5
        assert b.service_id == 7849893
        assert b.court_pool is None

    def test_pool_with_court_id_rejected(self) -> None:
        with pytest.raises(ValidationError, match="either court_pool"):
            _make_rule(court_pool="indoor", court_id=5)

    def test_pool_with_service_id_rejected(self) -> None:
        with pytest.raises(ValidationError, match="either court_pool"):
            _make_rule(court_pool="indoor", service_id=7849893)

    def test_pool_with_both_legacy_rejected(self) -> None:
        with pytest.raises(ValidationError, match="either court_pool"):
            _make_rule(court_pool="indoor", court_id=5, service_id=7849893)

    def test_neither_rejected(self) -> None:
        with pytest.raises(ValidationError, match="court_pool"):
            _make_rule()

    def test_court_id_only_rejected(self) -> None:
        with pytest.raises(ValidationError, match="BOTH"):
            _make_rule(court_id=5)

    def test_service_id_only_rejected(self) -> None:
        with pytest.raises(ValidationError, match="BOTH"):
            _make_rule(service_id=7849893)

    @pytest.mark.parametrize("name", ["indoor", "outdoor", "walls", "pool-1", "p_2", "abc123"])
    def test_pool_name_valid(self, name: str) -> None:
        b = _make_rule(court_pool=name)
        assert b.court_pool == name

    @pytest.mark.parametrize("name", ["Indoor", "POOL", "pool 1", "pool.1", "pool!", ""])
    def test_pool_name_invalid(self, name: str) -> None:
        with pytest.raises(ValidationError):
            _make_rule(court_pool=name)


# ---------- ResolvedBooking court_ids + repr --------------------------------


class TestResolvedBookingCourtIds:
    def test_single_court_repr_uses_brackets(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_ids=(5,),
            service_id=7849893,
            profile=p,
            enabled=True,
        )
        assert "courts=[5]" in repr(rb)
        assert "pool=" not in repr(rb)

    def test_three_courts_inline(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_ids=(1, 2, 3),
            service_id=7849893,
            profile=p,
            enabled=True,
        )
        assert "courts=[1,2,3]" in repr(rb)

    def test_seven_courts_uses_more_marker(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_ids=(1, 2, 3, 4, 5, 6, 7),
            service_id=7849893,
            profile=p,
            enabled=True,
            pool_name="indoor",
        )
        s = repr(rb)
        assert "courts=[1,+6 more]" in s
        assert "pool=indoor" in s

    def test_empty_court_ids_rejected(self) -> None:
        p = make_profile()
        with pytest.raises(ValidationError, match="court_ids"):
            ResolvedBooking(
                name="x",
                weekday=Weekday.FRIDAY,
                slot_local_time=time(18, 0),
                duration_minutes=60,
                court_ids=(),
                service_id=7849893,
                profile=p,
                enabled=True,
            )

    def test_duplicate_court_ids_rejected(self) -> None:
        p = make_profile()
        with pytest.raises(ValidationError, match="unique"):
            ResolvedBooking(
                name="x",
                weekday=Weekday.FRIDAY,
                slot_local_time=time(18, 0),
                duration_minutes=60,
                court_ids=(5, 6, 5),
                service_id=7849893,
                profile=p,
                enabled=True,
            )

    def test_pool_name_optional_default_none(self) -> None:
        p = make_profile()
        rb = ResolvedBooking(
            name="x",
            weekday=Weekday.FRIDAY,
            slot_local_time=time(18, 0),
            duration_minutes=60,
            court_ids=(5,),
            service_id=7849893,
            profile=p,
            enabled=True,
        )
        assert rb.pool_name is None
