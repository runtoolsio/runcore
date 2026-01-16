import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone, date, time, timedelta
from enum import Enum
from typing import Optional, Dict, Any

from dateutil.relativedelta import relativedelta

# Produced by ChatGPT - seems correct
ISO_DATE_TIME_PATTERN = re.compile(
    r'\b(\d{4}-\d{2}-\d{2}(?:T|\s)\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?:Z|[+-]\d{2}:\d{2})?)\b')


def unique_timestamp_hex(random_suffix_length=4):
    return secrets.token_hex(random_suffix_length) + format(int(datetime.utcnow().timestamp() * 1000000), 'x')[::-1]


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class TimeRange:
    """
    Represents a time duration range with optional minimum and maximum bounds.
    Used for matching execution time durations.

    Attributes:
        min: Minimum duration (inclusive)
        max: Maximum duration (inclusive)
    """
    min: Optional[timedelta] = None
    max: Optional[timedelta] = None

    def __call__(self, duration: timedelta) -> bool:
        """
        Check if a duration falls within this range.

        Args:
            duration: The duration to check

        Returns:
            True if duration is within range, False otherwise
        """
        if self.min is not None and duration < self.min:
            return False

        if self.max is not None and duration > self.max:
            return False

        return True

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'TimeRange':
        """
        Deserialize from a dictionary representation.

        Args:
            data: Dictionary with serialized range data.
                 Duration values should be in seconds.

        Returns:
            TimeRange: The deserialized range
        """
        return cls(
            min=timedelta(seconds=data['min']) if data.get('min') is not None else None,
            max=timedelta(seconds=data['max']) if data.get('max') is not None else None
        )

    def serialize(self) -> Dict[str, Any]:
        """
        Serialize to a dictionary representation.

        Returns:
            Dictionary with range data. Durations are converted to seconds.
        """
        data = {}
        if self.min is not None:
            data['min'] = self.min.total_seconds()
        if self.max is not None:
            data['max'] = self.max.total_seconds()
        return data

    def __str__(self) -> str:
        """String representation showing the range bounds."""
        parts = []
        if self.min is not None:
            parts.append(f">={self.min}")
        if self.max is not None:
            parts.append(f"<={self.max}")
        return f"[{' AND '.join(parts)}]" if parts else "[]"


def dtrange(since=None, until=None, *, until_incl=False) -> 'DateTimeRange':
    """
    Create a datetime range with optional bounds.

    Args:
        since: Start datetime (inclusive)
        until: End datetime
        until_incl: Whether end is inclusive (default: True)
    """
    return DateTimeRange(since, until, until_incl)


@dataclass
class DateTimeRange:
    """
    Represents a range of time with optional start and end points.
    By default, uses half-open intervals [start, end) where end is excluded.

    Attributes:
        since (Optional[datetime]): Start of the range, inclusive
        until (Optional[datetime]): End of the range
        until_included (bool): Whether the end point is excluded from the range (default False)
    """
    since: Optional[datetime] = None
    until: Optional[datetime] = None
    until_included: bool = False

    @classmethod
    def unbounded(cls) -> 'DateTimeRange':
        """
        Create an unbounded range that matches any non-None datetime.

        This is useful for checking existence of timestamps, e.g.,
        "has this run started?" without caring about when.

        Returns:
            DateTimeRange with no bounds
        """
        return cls()

    def __iter__(self):
        return iter((self.since, self.until, self.until_included))

    def __call__(self, tested_dt):
        return self.matches(tested_dt)

    def is_unbounded(self) -> bool:
        """
        Check if this range has no bounds (matches any non-None datetime).

        Returns:
            True if both since and until are None, False otherwise
        """
        return self.since is None and self.until is None

    def matches(self, tested_dt):
        """
        Check if a datetime falls within this range.

        Args:
            tested_dt: Datetime to test

        Returns:
            bool: True if the datetime is within the range
        """
        if not tested_dt:
            return False

        if self.since and tested_dt < self.since:
            return False

        if self.until:
            if self.until_included:
                if tested_dt > self.until:
                    return False
            else:
                if tested_dt >= self.until:
                    return False

        return True

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'DateTimeRange':
        """
        Create a DateTimeRange from a dictionary representation.

        Args:
            data: Dictionary containing serialized range data

        Returns:
            DateTimeRange: The deserialized range
        """
        return cls(
            since=parse(data['since']) if data.get('since') else None,
            until=parse(data['until']) if data.get('until') else None,
            until_included=data.get('until_included', False)
        )

    def serialize(self) -> Dict[str, Any]:
        """
        Convert this range to a dictionary representation.

        Returns:
            Dict[str, Any]: The serialized range
        """
        return {
            'since': format_dt_iso(self.since) if self.since else None,
            'until': format_dt_iso(self.until) if self.until else None,
            'until_included': self.until_included
        }

    @classmethod
    def parse_to_utc(cls, from_val, to_val):
        """
        Creates criteria with provided values converted to the UTC timezone.

        Args:
            from_val (str, datetime, date): The start date-time of the interval.
            to_val (str, datetime, date): The end date-time of the interval.
        """
        if from_val is None and to_val is None:
            raise ValueError('Both `from_val` and `to_val` parameters cannot be None')

        include_to = True

        if from_val is None:
            from_dt = None
        else:
            if isinstance(from_val, str):
                from_val = parse(from_val)
            if isinstance(from_val, datetime):
                from_dt = from_val.astimezone(timezone.utc)
            else:  # Assuming it is datetime.date
                from_dt = datetime.combine(from_val, time.min).astimezone(timezone.utc)

        if to_val is None:
            to_dt = None
        else:
            if isinstance(to_val, str):
                to_val = parse(to_val)
            if isinstance(to_val, datetime):
                to_dt = to_val.astimezone(timezone.utc)
            else:  # Assuming it is datetime.date
                to_dt = datetime.combine(to_val + timedelta(days=1), time.min).astimezone(timezone.utc)
                include_to = False

        return cls(from_dt, to_dt, include_to)

    @classmethod
    def single_day_range(cls, today_offset=0, *, to_utc=False) -> 'DateTimeRange':
        today = date.today()
        start_day = today + timedelta(days=today_offset)
        end_day = today + timedelta(days=today_offset + 1)
        start = datetime.combine(start_day, time.min)
        end = datetime.combine(end_day, time.min)

        return cls(to_naive_utc(start), to_naive_utc(end), False) if to_utc else cls(start, end, False)

    @classmethod
    def days_range(cls, days=0, *, to_utc=False):
        end = datetime.now()
        start = end + timedelta(days=days)

        if days > 0:
            start, end = end, start

        return cls(to_naive_utc(start), to_naive_utc(end), False) if to_utc else cls(start, end, False)

    @classmethod
    def today(cls, *, to_utc=False):
        return cls.single_day_range(0, to_utc=to_utc)

    @classmethod
    def yesterday(cls, *, to_utc=False):
        return cls.single_day_range(-1, to_utc=to_utc)

    @classmethod
    def week_back(cls, *, to_utc=False):
        return cls.days_range(-7, to_utc=to_utc)

    def __str__(self) -> str:
        """String representation of the range using mathematical interval notation."""
        since_str = str(self.since) if self.since else "-∞"
        until_str = str(self.until) if self.until else "∞"
        until_bracket = "]" if self.until_included else ")"
        return f"[{since_str}, {until_str}{until_bracket}"


def to_naive_utc(dt):
    return dt.astimezone().astimezone(timezone.utc).replace(tzinfo=None)


def parse(str_val):
    try:
        return parse_datetime(str_val)
    except ValueError:
        return date.fromisoformat(str_val)


def parse_datetime(str_ts):
    if not str_ts:
        return None

    sep = "T" if "T" in str_ts else " "

    if "." in str_ts:
        dec = ".%f"
    elif "," in str_ts:
        dec = ",%f"
    else:
        dec = ""

    zone = "%z" if any(1 for z in ('Z', '+') if z in str_ts) else ""

    try:
        return datetime.strptime(str_ts, "%Y-%m-%d" + sep + "%H:%M:%S" + dec + zone)
    except ValueError:
        return datetime.strptime(str_ts, "%Y-%m-%d" + sep + "%H:%M" + zone)


def parse_duration_to_sec(val):
    """
    Parse a duration string to seconds.

    Valid formats:
    - "123" - plain number treated as seconds
    - "123s" - seconds
    - "5m" - minutes
    - "2h" - hours
    - "1d" - days

    Args:
        val: Duration string to parse

    Returns:
        Float value in seconds

    Raises:
        ValueError: If the unit is unknown or format is invalid
    """
    # Handle plain number (no suffix)
    if val.isdigit() or (val.replace('.', '', 1).isdigit() and val.count('.') < 2):
        return float(val)

    if len(val) < 2:
        raise ValueError(f"Invalid duration format: {val}")

    try:
        value = float(val[:-1])
        unit = val[-1].lower()
    except ValueError:
        raise ValueError(f"Invalid duration format: {val}")

    if unit == 's':
        return value
    if unit == 'm':
        return value * 60
    if unit == 'h':
        return value * 60 * 60
    if unit == 'd':
        return value * 60 * 60 * 24

    raise ValueError("Unknown time unit: " + unit)


def parse_iso8601_duration(duration) -> relativedelta:
    match = re.match(r'P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?', duration)
    if not match:
        raise ValueError('Invalid duration: ' + duration)
    years = int(match.group(1)) if match.group(1) else 0
    months = int(match.group(2)) if match.group(2) else 0
    weeks = int(match.group(3)) if match.group(3) else 0
    days = int(match.group(4)) if match.group(4) else 0
    hours = int(match.group(5)) if match.group(5) else 0
    minutes = int(match.group(6)) if match.group(6) else 0
    seconds = int(match.group(7)) if match.group(7) else 0
    return relativedelta(
        years=years, months=months, weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds).normalized()


def format_timedelta(td, *, show_ms=True, null=''):
    if not td:
        return null

    mm, ss = divmod(td.seconds, 60)
    hh, mm = divmod(mm, 60)
    s = "%02d:%02d:%02d" % (hh, mm, ss)
    if td.days:
        def plural(n):
            return n, abs(n) != 1 and "s" or ""

        s = ("%d day%s, " % plural(td.days)) + s
    if show_ms and td.microseconds:
        s = s + (".%06d" % td.microseconds)[:-3]
        # s = s + ("%f" % (td.microseconds / 1000000))[1:-3]
    return s


def format_dt_iso(td):
    if td is None:
        return None
    return td.isoformat()


def format_dt_local_tz(dt, null='', *, include_ms=True):
    if not dt:
        return null

    ts = 'milliseconds' if include_ms else 'seconds'
    return dt.astimezone().replace(tzinfo=None).isoformat(sep=' ', timespec=ts)


def format_time_local_tz(dt, null='', include_ms=True):
    if not dt:
        return null

    if include_ms:
        return dt.astimezone().strftime('%H:%M:%S.%f')[:-3]
    else:
        return dt.astimezone().strftime('%H:%M:%S')


def format_dt_sql(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def parse_dt_sql(dt_str):
    return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')


class DateTimeFormat(Enum):
    DATE_TIME_MS_LOCAL_ZONE = (format_dt_local_tz,)
    TIME_MS_LOCAL_ZONE = (format_time_local_tz,)
    NONE = (lambda dt: None,)

    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)
