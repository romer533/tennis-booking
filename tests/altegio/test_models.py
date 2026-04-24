from __future__ import annotations

import pytest
from pydantic import ValidationError

from tennis_booking.altegio.models import (
    BookingAppointment,
    BookingRequest,
    BookingResponse,
)


def _make_appointment(**overrides: object) -> BookingAppointment:
    defaults: dict[str, object] = {
        "services": [7849893],
        "staff_id": 1521566,
        "datetime": "2026-04-26T23:00:00",
        "available_staff_ids": [1521566],
    }
    defaults.update(overrides)
    return BookingAppointment(**defaults)  # type: ignore[arg-type]


def _make_request(**overrides: object) -> BookingRequest:
    defaults: dict[str, object] = {
        "fullname": "Roman",
        "phone": "77026473809",
        "bookform_id": 551098,
        "appointments": [_make_appointment()],
    }
    defaults.update(overrides)
    return BookingRequest(**defaults)  # type: ignore[arg-type]


class TestBookingRequest:
    def test_valid_minimal(self) -> None:
        req = _make_request()
        wire = req.to_wire()
        assert wire["fullname"] == "Roman"
        assert wire["phone"] == "77026473809"
        assert wire["bookform_id"] == 551098
        assert wire["notify_by_sms"] == 1
        assert wire["is_charge_required_priority"] is True
        assert wire["is_support_charge"] is False
        assert wire["custom_fields"] == {}
        assert wire["appointments_charges"] == [
            {"id": 0, "services": [], "prepaid": []}
        ]

    def test_email_none_omits_key(self) -> None:
        wire = _make_request(email=None).to_wire()
        assert "email" not in wire

    def test_email_set_present(self) -> None:
        wire = _make_request(email="user@example.com").to_wire()
        assert wire["email"] == "user@example.com"

    def test_appointment_in_wire(self) -> None:
        wire = _make_request().to_wire()
        assert len(wire["appointments"]) == 1
        a = wire["appointments"][0]
        assert a["services"] == [7849893]
        assert a["staff_id"] == 1521566
        assert a["datetime"] == "2026-04-26T23:00:00"
        assert a["available_staff_ids"] == [1521566]
        assert a["id"] == 0
        assert a["chargeStatus"] == ""
        assert a["custom_fields"] == {}

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            BookingRequest(  # type: ignore[call-arg]
                fullname="X",
                phone="1",
                bookform_id=1,
                appointments=[_make_appointment()],
                surprise=42,
            )


class TestBookingAppointment:
    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            BookingAppointment(  # type: ignore[call-arg]
                services=[1],
                staff_id=1,
                datetime="2026-04-26T23:00:00",
                available_staff_ids=[1],
                some_extra="x",
            )


class TestBookingResponse:
    def test_parses_minimal(self) -> None:
        resp = BookingResponse.model_validate(
            {"record_id": 645268016, "record_hash": "abc"}
        )
        assert resp.record_id == 645268016
        assert resp.record_hash == "abc"

    def test_ignores_extra_fields(self) -> None:
        # Real Altegio responses include `record`, `id`, etc. — must not break us.
        resp = BookingResponse.model_validate(
            {
                "id": 0,
                "record_id": 1,
                "record_hash": "h",
                "record": {"foo": "bar"},
                "extra": "stuff",
            }
        )
        assert resp.record_id == 1

    def test_missing_record_id_raises(self) -> None:
        with pytest.raises(ValidationError):
            BookingResponse.model_validate({"record_hash": "h"})

    def test_missing_record_hash_raises(self) -> None:
        with pytest.raises(ValidationError):
            BookingResponse.model_validate({"record_id": 1})
