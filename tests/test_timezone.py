import datetime
import pytest
from zoneinfo import ZoneInfo
from starrocks_br import timezone


def test_should_get_current_time_in_cluster_tz_with_named_timezone():
    """Test getting current time in a named timezone."""
    result = timezone.get_current_time_in_cluster_tz("Asia/Shanghai")
    
    assert isinstance(result, str)
    assert len(result) == 19
    assert result.count("-") == 2
    assert result.count(":") == 2
    assert result.count(" ") == 1
    
    parsed_dt = datetime.datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
    assert isinstance(parsed_dt, datetime.datetime)
    
    tz_aware_dt = timezone.parse_datetime_with_tz(result, "Asia/Shanghai")
    assert tz_aware_dt.tzinfo is not None


def test_should_get_current_time_in_cluster_tz_with_utc():
    """Test getting current time in UTC."""
    result = timezone.get_current_time_in_cluster_tz("UTC")
    
    assert isinstance(result, str)
    assert len(result) == 19
    
    parsed_dt = datetime.datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
    assert isinstance(parsed_dt, datetime.datetime)
    
    tz_aware_dt = timezone.parse_datetime_with_tz(result, "UTC")
    assert tz_aware_dt.tzinfo is not None


def test_should_get_current_time_in_cluster_tz_with_offset():
    """Test getting current time with offset timezone."""
    result = timezone.get_current_time_in_cluster_tz("+08:00")
    
    assert isinstance(result, str)
    assert len(result) == 19
    
    parsed_dt = datetime.datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
    assert isinstance(parsed_dt, datetime.datetime)
    
    tz_aware_dt = timezone.parse_datetime_with_tz(result, "+08:00")
    assert tz_aware_dt.tzinfo is not None


def test_should_apply_timezone_to_current_time():
    """Test that get_current_time_in_cluster_tz applies timezone correctly.
    
    This verifies that the timezone parameter actually affects the result.
    We test by getting the same moment in two different timezones and verifying
    they represent the same instant when normalized to UTC (allowing for small
    call time differences).
    """
    utc_result = timezone.get_current_time_in_cluster_tz("UTC")
    shanghai_result = timezone.get_current_time_in_cluster_tz("Asia/Shanghai")
    
    utc_dt = timezone.parse_datetime_with_tz(utc_result, "UTC")
    shanghai_dt = timezone.parse_datetime_with_tz(shanghai_result, "Asia/Shanghai")
    
    utc_normalized = utc_dt.astimezone(ZoneInfo("UTC"))
    shanghai_normalized = shanghai_dt.astimezone(ZoneInfo("UTC"))
    
    time_diff = abs((utc_normalized - shanghai_normalized).total_seconds())
    assert time_diff < 2


def test_should_parse_datetime_with_named_timezone():
    """Test parsing datetime string with named timezone."""
    dt_str = "2025-10-15 10:30:00"
    tz = "Asia/Shanghai"
    
    result = timezone.parse_datetime_with_tz(dt_str, tz)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.strftime("%Y-%m-%d %H:%M:%S") == dt_str


def test_should_parse_datetime_with_utc():
    """Test parsing datetime string with UTC."""
    dt_str = "2025-10-15 10:30:00"
    tz = "UTC"
    
    result = timezone.parse_datetime_with_tz(dt_str, tz)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.strftime("%Y-%m-%d %H:%M:%S") == dt_str


def test_should_parse_datetime_with_offset():
    """Test parsing datetime string with offset timezone."""
    dt_str = "2025-10-15 10:30:00"
    tz = "+08:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, tz)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.strftime("%Y-%m-%d %H:%M:%S") == dt_str


def test_should_normalize_timezone_aware_datetime_to_target_tz():
    """Test normalizing timezone-aware datetime to target timezone."""
    utc_dt = datetime.datetime(2025, 10, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
    target_tz = "Asia/Shanghai"
    
    result = timezone.normalize_datetime_to_tz(utc_dt, target_tz)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.tzinfo != utc_dt.tzinfo


def test_should_normalize_naive_datetime_to_target_tz():
    """Test normalizing naive datetime to target timezone."""
    naive_dt = datetime.datetime(2025, 10, 15, 10, 30, 0)
    target_tz = "UTC"
    
    result = timezone.normalize_datetime_to_tz(naive_dt, target_tz)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None


def test_should_handle_invalid_timezone_string():
    """Test that invalid timezone strings default to UTC."""
    dt_str = "2025-10-15 10:30:00"
    invalid_tz = "Invalid/Timezone"
    
    result = timezone.parse_datetime_with_tz(dt_str, invalid_tz)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    
    utc_result = timezone.parse_datetime_with_tz(dt_str, "UTC")
    assert result.tzinfo == utc_result.tzinfo
    assert result == utc_result


def test_should_compare_datetimes_in_same_timezone():
    """Test that datetime comparison works correctly when both are in the same timezone."""
    tz = "Asia/Shanghai"
    dt1_str = "2025-10-15 10:00:00"
    dt2_str = "2025-10-15 11:00:00"
    
    dt1 = timezone.parse_datetime_with_tz(dt1_str, tz)
    dt2 = timezone.parse_datetime_with_tz(dt2_str, tz)
    
    assert dt1 < dt2
    assert dt2 > dt1


def test_should_compare_datetimes_across_timezones():
    """Test that datetime comparison works correctly across different timezones."""
    dt1_str = "2025-10-15 10:00:00"
    dt2_str = "2025-10-15 18:00:00"
    
    dt1 = timezone.parse_datetime_with_tz(dt1_str, "UTC")
    dt2 = timezone.parse_datetime_with_tz(dt2_str, "Asia/Shanghai")
    
    assert dt1 == dt2
    assert dt1 <= dt2


@pytest.mark.parametrize("invalid_offset", [
    "+08",           # Too short (less than 6 characters)
    "+8:00",         # Missing leading zero in hours (will cause ValueError when parsing)
    "+AB:CD",        # Invalid characters (non-numeric)
    "+0800",         # Missing colon at position 3
    "+",             # Empty offset string (only sign)
    "+08:",          # Missing minutes (will cause ValueError when parsing minutes)
    ":00",           # Missing sign and hours
    "08:00",         # Missing sign (doesn't start with + or -)
    "~08:00",        # Invalid sign character
    "*08:00",        # Invalid sign character
    "+25:00",        # Invalid hour value (hours must be 00-23)
    "+08:60",        # Invalid minute value (minutes must be 00-59)
    "+08:99",        # Invalid minute value
    "+24:00",        # Invalid hour value (hours must be < 24)
    "-25:00",        # Invalid hour value (negative)
    "+08:-10",       # Invalid minute value (negative)
])
def test_should_handle_invalid_offset_strings_default_to_utc(invalid_offset):
    """Test that invalid offset strings default to UTC.
    
    All these offset strings should cause ValueError during parsing,
    which is caught and defaults to UTC timezone.
    We validate ISO 8601 format: hours must be 00-23, minutes must be 00-59.
    """
    dt_str = "2025-10-15 10:30:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, invalid_offset)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    
    utc_result = timezone.parse_datetime_with_tz(dt_str, "UTC")
    assert result.tzinfo == utc_result.tzinfo
    assert result == utc_result


@pytest.mark.parametrize("valid_offset", [
    "-12:00",        # Negative offset boundary
    "+14:00",        # Positive offset boundary
    "+05:30",        # Offset with non-zero minutes
    "-05:30",        # Negative offset with minutes
    "+09:45",        # Offset with minutes
    "-03:15",        # Negative offset with minutes
    "+00:00",        # Zero offset (positive)
    "-00:00",        # Zero offset (negative, should be treated as UTC)
])
def test_should_handle_valid_offset_strings(valid_offset):
    """Test handling of valid offset strings.
    
    All these offset strings should be parsed successfully and create
    a timezone-aware datetime object.
    """
    dt_str = "2025-10-15 10:30:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, valid_offset)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.strftime("%Y-%m-%d %H:%M:%S") == dt_str


def test_should_handle_zero_offset_variations():
    """Test that zero offset variations all map to UTC."""
    dt_str = "2025-10-15 10:30:00"
    
    result1 = timezone.parse_datetime_with_tz(dt_str, "+00:00")
    result2 = timezone.parse_datetime_with_tz(dt_str, "-00:00")
    result3 = timezone.parse_datetime_with_tz(dt_str, "UTC")
    
    assert isinstance(result1, datetime.datetime)
    assert isinstance(result2, datetime.datetime)
    assert isinstance(result3, datetime.datetime)
    assert result1.tzinfo == result2.tzinfo
    assert result1.tzinfo == result3.tzinfo


@pytest.mark.parametrize("invalid_timezone", [
    "   ",                      # Whitespace only
    "Nonexistent/Timezone",     # Nonexistent timezone name
    "Asia_Shanghai",            # Malformed (underscore instead of slash)
    "Invalid/Timezone/Name",    # Invalid timezone name
    "NotA/Real/Zone",           # Fake timezone
    "America/NewYork",          # Wrong capitalization (should be America/New_York)
    "",                         # Empty string
])
def test_should_handle_invalid_timezone_names_default_to_utc(invalid_timezone):
    """Test that invalid timezone names default to UTC.
    
    All these timezone strings should cause ZoneInfo to raise an exception,
    which is caught and defaults to UTC timezone.
    """
    dt_str = "2025-10-15 10:30:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, invalid_timezone)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    
    utc_result = timezone.parse_datetime_with_tz(dt_str, "UTC")
    assert result.tzinfo == utc_result.tzinfo
    assert result == utc_result


@pytest.mark.parametrize("timezone_with_whitespace", [
    "  Asia/Shanghai  ",
    "  +08:00  ",
    " UTC ",
    "  America/New_York  ",
    "\t+05:30\t",
    "\nAsia/Shanghai\n",
])
def test_should_trim_whitespace_from_timezone_strings(timezone_with_whitespace):
    """Test that timezone strings with leading/trailing whitespace are properly trimmed."""
    dt_str = "2025-10-15 10:30:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, timezone_with_whitespace)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    assert result.strftime("%Y-%m-%d %H:%M:%S") == dt_str


@pytest.mark.parametrize("invalid_timezone", [
    "Invalid/Timezone/Name",
    "NotA/Real/Zone",
    "   ",
    "",
])
def test_should_handle_invalid_timezone_in_get_current_time(invalid_timezone):
    """Test that get_current_time_in_cluster_tz handles invalid timezone gracefully.
    
    Invalid timezones should default to UTC, so the result should be a valid
    datetime string that can be parsed and represents a valid timestamp.
    Since both invalid timezone and explicit UTC default to UTC, we verify
    the format is correct and the timezone handling works.
    """
    result = timezone.get_current_time_in_cluster_tz(invalid_timezone)
    
    assert isinstance(result, str)
    assert len(result) == 19
    assert result.count("-") == 2
    assert result.count(":") == 2
    
    parsed_dt = datetime.datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
    assert isinstance(parsed_dt, datetime.datetime)
    
    tz_aware_dt = timezone.parse_datetime_with_tz(result, "UTC")
    assert tz_aware_dt.tzinfo is not None
    
    utc_result = timezone.get_current_time_in_cluster_tz("UTC")
    utc_parsed = datetime.datetime.strptime(utc_result, "%Y-%m-%d %H:%M:%S")
    parsed = datetime.datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
    
    time_diff = abs((parsed - utc_parsed).total_seconds())
    assert time_diff < 2


@pytest.mark.parametrize("invalid_timezone", [
    "Invalid/Timezone",
    "NotA/Real/Zone",
    "   ",
    "",
])
def test_should_handle_invalid_timezone_in_normalize(invalid_timezone):
    """Test that normalize_datetime_to_tz handles invalid timezone gracefully."""
    dt = datetime.datetime(2025, 10, 15, 10, 30, 0)
    
    result = timezone.normalize_datetime_to_tz(dt, invalid_timezone)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    
    utc_result = timezone.normalize_datetime_to_tz(dt, "UTC")
    assert result.tzinfo == utc_result.tzinfo
    assert result == utc_result


@pytest.mark.parametrize("offset_without_colon", [
    "+0800",
    "+1234",
    "-0500",
    "+1400",
    "+08X00",
    "+12-34",
    "-05.00",
])
def test_should_reject_offset_strings_without_colon(offset_without_colon):
    """Test that offset strings without colon at position 3 default to UTC."""
    dt_str = "2025-10-15 10:30:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, offset_without_colon)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    
    utc_result = timezone.parse_datetime_with_tz(dt_str, "UTC")
    assert result.tzinfo == utc_result.tzinfo
    assert result == utc_result


@pytest.mark.parametrize("offset_with_invalid_sign", [
    "08:00",
    "~08:00",
    "*08:00",
    "@08:00",
    "#08:00",
    "$08:00",
    "%08:00",
    "^08:00",
])
def test_should_reject_offset_strings_without_valid_sign(offset_with_invalid_sign):
    """Test that offset strings without '+' or '-' sign default to UTC."""
    dt_str = "2025-10-15 10:30:00"
    
    result = timezone.parse_datetime_with_tz(dt_str, offset_with_invalid_sign)
    
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is not None
    
    utc_result = timezone.parse_datetime_with_tz(dt_str, "UTC")
    assert result.tzinfo == utc_result.tzinfo
    assert result == utc_result

