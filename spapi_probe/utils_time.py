from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo
from typing import Tuple

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def day_window_utc(tz_name: str, snapshot_day: date) -> Tuple[str, str]:
    tz = ZoneInfo(tz_name)
    start_local = datetime(snapshot_day.year, snapshot_day.month, snapshot_day.day, 0, 0, 0, tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return iso_z(start_local), iso_z(end_local)

def yesterday_local(tz_name: str) -> date:
    now_local = datetime.now(ZoneInfo(tz_name))
    return now_local.date() - timedelta(days=1)
