from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

ATYRAU = ZoneInfo("Asia/Atyrau")
OPEN_LOCAL_TIME = time(7, 0, 0)
LEAD_DAYS = 3


def next_open_window(slot_local_dt: datetime) -> datetime:
    """Возвращает момент открытия окна бронирования слота в UTC."""
    if slot_local_dt.tzinfo is None:
        raise ValueError("slot_local_dt must be timezone-aware")
    if slot_local_dt.tzinfo != ATYRAU:
        raise ValueError(f"slot_local_dt must be in Asia/Atyrau, got {slot_local_dt.tzinfo}")

    open_local = datetime.combine(
        slot_local_dt.date() - timedelta(days=LEAD_DAYS),
        OPEN_LOCAL_TIME,
        tzinfo=ATYRAU,
    )
    return open_local.astimezone(UTC)
