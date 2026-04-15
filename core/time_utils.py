"""
Stratos1 - Time formatting utilities.

All internal timestamps are stored in UTC (or timezone-aware datetime objects).
Display formatting converts to the configured timezone (default: Europe/Stockholm)
using the project's custom AM/PM rules:

    AM hours     -> zero-padded  HH:MM AM   (e.g. 02:06 AM, 09:45 AM)
    PM hours 1-9 -> single digit  H:MM PM   (e.g. 2:06 PM, 7:30 PM)
    PM 10-12     -> two digits   HH:MM PM   (e.g. 10:15 PM, 12:00 PM)

Minutes are always zero-padded to two digits.
"""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Default display timezone
_DEFAULT_TZ = "Europe/Stockholm"


def format_time(
    dt: datetime,
    tz: str = _DEFAULT_TZ,
) -> str:
    """
    Format a datetime using the Stratos1 custom AM/PM convention.

    Parameters
    ----------
    dt:
        A datetime object. If naive (no tzinfo), it is assumed to already be
        in the target timezone.  If aware, it is converted to *tz* first.
    tz:
        IANA timezone name for display (e.g. "Europe/Stockholm").

    Returns
    -------
    str
        Formatted time string such as "02:06 AM" or "7:30 PM".
    """
    # Convert aware datetimes to the target timezone; leave naive ones as-is
    # (assumed to already be in the display timezone).
    if dt.tzinfo is not None:
        target_tz = ZoneInfo(tz)
        dt = dt.astimezone(target_tz)

    hour_24 = dt.hour
    minute = dt.minute

    # Determine AM / PM and the 12-hour value.
    if hour_24 == 0:
        hour_12 = 12
        period = "AM"
    elif hour_24 < 12:
        hour_12 = hour_24
        period = "AM"
    elif hour_24 == 12:
        hour_12 = 12
        period = "PM"
    else:
        hour_12 = hour_24 - 12
        period = "PM"

    # Apply the custom width rules.
    if period == "AM":
        # AM: always zero-padded to two digits
        hour_str = f"{hour_12:02d}"
    else:
        # PM 1-9: single digit; PM 10-12: two digits
        if hour_12 < 10:
            hour_str = str(hour_12)
        else:
            hour_str = f"{hour_12:02d}"

    return f"{hour_str}:{minute:02d} {period}"


def now_utc() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def now_local(tz: str = _DEFAULT_TZ) -> datetime:
    """Return the current time in the given timezone."""
    return datetime.now(ZoneInfo(tz))


def to_local(dt: datetime, tz: str = _DEFAULT_TZ) -> datetime:
    """
    Convert a timezone-aware datetime to the specified local timezone.

    If the datetime is naive, it is assumed to be UTC before conversion.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo(tz))
