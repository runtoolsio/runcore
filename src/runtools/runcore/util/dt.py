import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone, date, time, timedelta
from enum import Enum
from typing import Optional

from dateutil.relativedelta import relativedelta

# Produced by ChatGPT - seems correct
ISO_DATE_TIME_PATTERN = re.compile(
    r'\b(\d{4}-\d{2}-\d{2}(?:T|\s)\d{2}:\d{2}:\d{2}(?:\.\d{3})?(?:Z|[+-]\d{2}:\d{2})?)\b')


def unique_timestamp_hex(random_suffix_length=4):
    return secrets.token_hex(random_suffix_length) + format(int(datetime.utcnow().timestamp() * 1000000), 'x')[::-1]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DateTimeRange:
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    end_included: bool = True

    def __iter__(self):
        return iter((self.start, self.end, self.end_included))

    def __bool__(self):
        return bool(self.start) or bool(self.end)

    def __call__(self, tested_dt):
        return self.matches(tested_dt)

    def matches(self, tested_dt):
        if not tested_dt:
            return not bool(self)

        if self.start and tested_dt < self.start:
            return False
        if self.end:
            if self.end_included:
                if tested_dt > self.end:
                    return False
            else:
                if tested_dt >= self.end:
                    return False

        return True


def parse_range_to_utc(from_val, to_val):
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

    return DateTimeRange(from_dt, to_dt, include_to)


def single_day_range(day_offset=0, *, to_utc=False) -> DateTimeRange:
    today = date.today()
    start_day = today + timedelta(days=day_offset)
    end_day = today + timedelta(days=day_offset + 1)
    start = datetime.combine(start_day, time.min)
    end = datetime.combine(end_day, time.min)

    return DateTimeRange(to_naive_utc(start), to_naive_utc(end), False) if to_utc else DateTimeRange(start, end, False)


def days_range(days=0, *, to_utc=False):
    end = datetime.now()
    start = end + timedelta(days=days)

    if days > 0:
        start, end = end, start

    return DateTimeRange(to_naive_utc(start), to_naive_utc(end), False) if to_utc else DateTimeRange(start, end, False)


def to_naive_utc(dt):
    return dt.astimezone().astimezone(timezone.utc).replace(tzinfo=None)  # TODO better timezone aware?


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
    value = float(val[:-1])
    unit = val[-1].lower()

    if unit == 's':
        return value
    if unit == 'm':
        return value * 60
    if unit == 'h':
        return value * 60 * 60
    if unit == 'd':
        return value * 60 * 60 * 24

    raise ValueError("Unknown unit: " + unit)


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
