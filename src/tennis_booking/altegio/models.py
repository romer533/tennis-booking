from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class BookingAppointment(BaseModel):
    """Один appointment в массиве /book_record. id=0 для новой брони."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    services: list[int]
    staff_id: int
    datetime: str  # "YYYY-MM-DDTHH:MM:SS" в локальном времени клуба, без TZ-суффикса
    available_staff_ids: list[int]
    id: int = 0
    chargeStatus: str = ""  # noqa: N815 — wire-формат поля Altegio (camelCase)
    custom_fields: dict[str, Any] = Field(default_factory=dict)


class BookingRequest(BaseModel):
    """Body для POST /api/v1/book_record/{company_id}."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    fullname: str
    phone: str
    bookform_id: int
    appointments: list[BookingAppointment]
    email: str | None = None
    notify_by_sms: int = 1
    is_charge_required_priority: bool = True
    is_support_charge: bool = False
    appointments_charges: list[dict[str, Any]] = Field(
        default_factory=lambda: [{"id": 0, "services": [], "prepaid": []}]
    )
    custom_fields: dict[str, Any] = Field(default_factory=dict)

    def to_wire(self) -> dict[str, Any]:
        """JSON-ready dict. email=None → ключ не включается."""
        data = self.model_dump(mode="json")
        if data.get("email") is None:
            data.pop("email", None)
        return data


class BookingResponse(BaseModel):
    """Парсится из первого элемента массива-ответа /book_record."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    record_id: int
    record_hash: str
