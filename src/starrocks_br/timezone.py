import datetime
from zoneinfo import ZoneInfo


def get_current_time_in_cluster_tz(cluster_tz: str) -> str:
    """Get current time formatted in cluster timezone.

    Args:
        cluster_tz: Timezone string (e.g., 'Asia/Shanghai', 'UTC', '+08:00')

    Returns:
        Formatted datetime string in 'YYYY-MM-DD HH:MM:SS' format in the cluster timezone
    """
    tz = _get_timezone(cluster_tz)
    now = datetime.datetime.now(tz)
    return now.strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime_with_tz(dt_str: str, tz: str) -> datetime.datetime:
    """Parse datetime string assuming the given timezone.

    Args:
        dt_str: Datetime string in 'YYYY-MM-DD HH:MM:SS' format
        tz: Timezone string (e.g., 'Asia/Shanghai', 'UTC', '+08:00')

    Returns:
        Timezone-aware datetime object
    """
    timezone = _get_timezone(tz)

    dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone)

    return dt


def normalize_datetime_to_tz(dt: datetime.datetime, target_tz: str) -> datetime.datetime:
    """Convert datetime to target timezone.

    Args:
        dt: Datetime object (timezone-aware or naive)
        target_tz: Target timezone string (e.g., 'Asia/Shanghai', 'UTC', '+08:00')

    Returns:
        Timezone-aware datetime object in the target timezone
    """
    timezone = _get_timezone(target_tz)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    dt = dt.astimezone(timezone)

    return dt


def _get_timezone(tz_str: str) -> ZoneInfo | datetime.timezone:
    """Get timezone object from timezone string.

    Handles both named timezones (e.g., 'Asia/Shanghai') and offset strings (e.g., '+08:00', '-05:00').

    Args:
        tz_str: Timezone string

    Returns:
        ZoneInfo or timezone object
    """
    tz_str = tz_str.strip()

    if tz_str.upper() == "UTC" or tz_str == "+00:00" or tz_str == "-00:00":
        return ZoneInfo("UTC")

    if tz_str.startswith(("+", "-")):
        try:
            hours, minutes = _parse_offset(tz_str)
            offset = datetime.timedelta(hours=hours, minutes=minutes)
            return datetime.timezone(offset)
        except ValueError:
            return ZoneInfo("UTC")

    try:
        return ZoneInfo(tz_str)
    except Exception:
        return ZoneInfo("UTC")


def _parse_offset(offset_str: str) -> tuple[int, int]:
    """Parse timezone offset string to hours and minutes.

    Args:
        offset_str: Offset string in format '+HH:MM' or '-HH:MM'

    Returns:
        Tuple of (hours, minutes)

    Raises:
        ValueError: If offset string is invalid, including:
            - String length < 6 characters
            - Invalid format (missing colon, invalid characters)
            - Hours >= 24 or < 0
            - Minutes >= 60 or < 0
    """
    if len(offset_str) < 6:
        raise ValueError(f"Invalid offset format: {offset_str}")

    if offset_str[3] != ":":
        raise ValueError(f"Invalid offset format: {offset_str} (missing colon)")

    sign = 1 if offset_str[0] == "+" else -1

    try:
        hours = int(offset_str[1:3])
        minutes = int(offset_str[4:6])
    except ValueError as e:
        raise ValueError(f"Invalid offset format: {offset_str} (non-numeric values)") from e

    if hours < 0 or hours >= 24:
        raise ValueError(f"Invalid offset format: {offset_str} (hours must be 00-23)")

    if minutes < 0 or minutes >= 60:
        raise ValueError(f"Invalid offset format: {offset_str} (minutes must be 00-59)")

    return sign * hours, sign * minutes
