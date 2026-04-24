"""Append-only JSONL persistence for successful bookings + dedup.

Goal: survive restart and avoid double-booking when a slot was already
captured (by an earlier attempt of this service or by a manual booking
imported via CLI). Не replacement для server-side state — это локальный
кэш состояний, в которых сервис уверен.
"""
from __future__ import annotations

from .models import BookedSlot, BookingPhase
from .store import BookingStore, FileBookingStore, MemoryBookingStore

__all__ = [
    "BookedSlot",
    "BookingPhase",
    "BookingStore",
    "FileBookingStore",
    "MemoryBookingStore",
]
