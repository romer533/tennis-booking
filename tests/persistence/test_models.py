from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from tennis_booking.common.tz import ALMATY
from tennis_booking.persistence.models import SCHEMA_VERSION, BookedSlot

SLOT_LOCAL = datetime(2026, 4, 26, 18, 0, tzinfo=ALMATY)
BOOKED_AT = datetime(2026, 4, 23, 2, 0, tzinfo=UTC)


def _kwargs(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        schema_version=SCHEMA_VERSION,
        record_id=12345,
        record_hash="abc123hash",
        slot_dt_local=SLOT_LOCAL,
        court_id=7,
        service_id=99,
        profile_name="roman",
        phase="window",
        booked_at_utc=BOOKED_AT,
    )
    base.update(overrides)
    return base


# ---- Construction validation ------------------------------------------------


def test_valid_slot_constructs() -> None:
    slot = BookedSlot(**_kwargs())
    assert slot.record_id == 12345
    assert slot.phase == "window"


def test_naive_slot_dt_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        BookedSlot(**_kwargs(slot_dt_local=datetime(2026, 4, 26, 18, 0)))


def test_utc_slot_dt_rejected() -> None:
    with pytest.raises(ValueError, match="Asia/Almaty"):
        BookedSlot(**_kwargs(slot_dt_local=datetime(2026, 4, 26, 13, 0, tzinfo=UTC)))


def test_fixed_offset_slot_dt_rejected_even_if_plus_05() -> None:
    fixed_5 = timezone(timedelta(hours=5))
    with pytest.raises(ValueError, match="Asia/Almaty"):
        BookedSlot(**_kwargs(slot_dt_local=datetime(2026, 4, 26, 18, 0, tzinfo=fixed_5)))


def test_naive_booked_at_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        BookedSlot(**_kwargs(booked_at_utc=datetime(2026, 4, 23, 2, 0)))


def test_non_utc_booked_at_rejected() -> None:
    with pytest.raises(ValueError, match="UTC"):
        BookedSlot(**_kwargs(booked_at_utc=datetime(2026, 4, 23, 7, 0, tzinfo=ALMATY)))


def test_zero_record_id_rejected() -> None:
    with pytest.raises(ValueError, match="record_id"):
        BookedSlot(**_kwargs(record_id=0))


def test_empty_record_hash_rejected() -> None:
    with pytest.raises(ValueError, match="record_hash"):
        BookedSlot(**_kwargs(record_hash=""))


def test_zero_court_id_rejected() -> None:
    with pytest.raises(ValueError, match="court_id"):
        BookedSlot(**_kwargs(court_id=0))


def test_zero_service_id_rejected() -> None:
    with pytest.raises(ValueError, match="service_id"):
        BookedSlot(**_kwargs(service_id=0))


def test_invalid_profile_name_rejected() -> None:
    with pytest.raises(ValueError, match="profile_name"):
        BookedSlot(**_kwargs(profile_name="Roman Capital"))


def test_invalid_phase_rejected() -> None:
    with pytest.raises(ValueError, match="phase"):
        BookedSlot(**_kwargs(phase="invalid"))


def test_phase_manual_accepted() -> None:
    slot = BookedSlot(**_kwargs(phase="manual"))
    assert slot.phase == "manual"


def test_phase_poll_accepted() -> None:
    slot = BookedSlot(**_kwargs(phase="poll"))
    assert slot.phase == "poll"


def test_wrong_schema_version_rejected() -> None:
    with pytest.raises(ValueError, match="schema_version"):
        BookedSlot(**_kwargs(schema_version=2))


# ---- Serialization round-trip -----------------------------------------------


def test_to_dict_contains_all_fields() -> None:
    slot = BookedSlot(**_kwargs())
    d = slot.to_dict()
    assert d["schema_version"] == 1
    assert d["record_id"] == 12345
    assert d["record_hash"] == "abc123hash"
    assert d["court_id"] == 7
    assert d["service_id"] == 99
    assert d["profile_name"] == "roman"
    assert d["phase"] == "window"
    # ISO with TZ-suffix in Almaty (+05:00) and UTC (+00:00).
    assert d["slot_dt_local"].endswith("+05:00")
    assert d["booked_at_utc"].endswith("+00:00")


def test_round_trip_preserves_values() -> None:
    original = BookedSlot(**_kwargs())
    d = original.to_dict()
    restored = BookedSlot.from_dict(d)
    assert restored == original


def test_from_dict_naive_slot_raises() -> None:
    d = BookedSlot(**_kwargs()).to_dict()
    d["slot_dt_local"] = "2026-04-26T18:00:00"  # no offset
    with pytest.raises(ValueError, match="tzinfo"):
        BookedSlot.from_dict(d)


def test_from_dict_naive_booked_at_raises() -> None:
    d = BookedSlot(**_kwargs()).to_dict()
    d["booked_at_utc"] = "2026-04-23T02:00:00"
    with pytest.raises(ValueError, match="tzinfo"):
        BookedSlot.from_dict(d)


def test_from_dict_missing_field_raises() -> None:
    d = BookedSlot(**_kwargs()).to_dict()
    d.pop("record_id")
    with pytest.raises(ValueError, match="record_id"):
        BookedSlot.from_dict(d)


def test_from_dict_non_iso_raises() -> None:
    d = BookedSlot(**_kwargs()).to_dict()
    d["slot_dt_local"] = "not-an-iso-date"
    with pytest.raises(ValueError, match="ISO 8601"):
        BookedSlot.from_dict(d)
