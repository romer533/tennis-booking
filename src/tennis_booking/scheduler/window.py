from datetime import UTC, datetime, time, timedelta

from tennis_booking.common.tz import ALMATY

OPEN_LOCAL_TIME = time(7, 0, 0)
# Altegio booking horizon = today + 2 calendar days (empirically confirmed via
# `search_dates` API). Slot D opens on day D-2 at 07:00 Asia/Almaty.
LEAD_DAYS = 2

__all__ = ["ALMATY", "LEAD_DAYS", "OPEN_LOCAL_TIME", "next_open_window"]


def next_open_window(slot_local_dt: datetime) -> datetime:
    """Возвращает момент открытия окна бронирования слота в UTC."""
    if slot_local_dt.tzinfo is None:
        raise ValueError("slot_local_dt must be timezone-aware")
    if slot_local_dt.tzinfo != ALMATY:
        raise ValueError(f"slot_local_dt must be in Asia/Almaty, got {slot_local_dt.tzinfo}")

    open_local = datetime.combine(
        slot_local_dt.date() - timedelta(days=LEAD_DAYS),
        OPEN_LOCAL_TIME,
        tzinfo=ALMATY,
    )
    return open_local.astimezone(UTC)
