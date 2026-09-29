# https://pypi.org/project/humanfriendly/

import datetime
import decimal
import numbers
import re
from typing import Any, List, NamedTuple, Optional, Union

if True:  # from humanfriendly/__init__.py
    SizeUnit = NamedTuple('SizeUnit', [('divider', int), ('symbol', str), ('name', str)])
    CombinedUnit = NamedTuple('CombinedUnit', [('decimal', SizeUnit), ('binary', SizeUnit)])

    # Common disk size units in binary (base-2) and decimal (base-10) multiples.
    disk_size_units = (
        CombinedUnit(SizeUnit(1000**1, 'KB', 'kilobyte'), SizeUnit(1024**1, 'KiB', 'kibibyte')),
        CombinedUnit(SizeUnit(1000**2, 'MB', 'megabyte'), SizeUnit(1024**2, 'MiB', 'mebibyte')),
        CombinedUnit(SizeUnit(1000**3, 'GB', 'gigabyte'), SizeUnit(1024**3, 'GiB', 'gibibyte')),
        CombinedUnit(SizeUnit(1000**4, 'TB', 'terabyte'), SizeUnit(1024**4, 'TiB', 'tebibyte')),
        CombinedUnit(SizeUnit(1000**5, 'PB', 'petabyte'), SizeUnit(1024**5, 'PiB', 'pebibyte')),
        CombinedUnit(SizeUnit(1000**6, 'EB', 'exabyte'), SizeUnit(1024**6, 'EiB', 'exbibyte')),
        CombinedUnit(SizeUnit(1000**7, 'ZB', 'zettabyte'), SizeUnit(1024**7, 'ZiB', 'zebibyte')),
        CombinedUnit(SizeUnit(1000**8, 'YB', 'yottabyte'), SizeUnit(1024**8, 'YiB', 'yobibyte')),
    )

    TimeUnit = NamedTuple('TimeUnit', [('divider', float), ('singular', str), ('plural', str), ('abbreviations', List[str])])
    time_units = (
        TimeUnit(divider=1e-9, singular='nanosecond', plural='nanoseconds', abbreviations=['ns']),
        TimeUnit(divider=1e-6, singular='microsecond', plural='microseconds', abbreviations=['us']),
        TimeUnit(divider=1e-3, singular='millisecond', plural='milliseconds', abbreviations=['ms']),
        TimeUnit(divider=1, singular='second', plural='seconds', abbreviations=['s', 'sec', 'secs']),
        TimeUnit(divider=60, singular='minute', plural='minutes', abbreviations=['m', 'min', 'mins']),
        TimeUnit(divider=60 * 60, singular='hour', plural='hours', abbreviations=['h']),
        TimeUnit(divider=60 * 60 * 24, singular='day', plural='days', abbreviations=['d']),
        TimeUnit(divider=60 * 60 * 24 * 7, singular='week', plural='weeks', abbreviations=['w']),
        TimeUnit(divider=60 * 60 * 24 * 7 * 52, singular='year', plural='years', abbreviations=['y'])
    )

    def coerce_boolean(value: Optional[str]) -> bool:
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ('1', 'yes', 'true', 'on'):
                return True
            elif normalized in ('0', 'no', 'false', 'off', ''):
                return False
            else:
                msg = "Failed to coerce string to boolean! (%r)"
                raise ValueError(format(msg, value))
        else:
            return bool(value)

    def coerce_seconds(value: Any):
        if isinstance(value, datetime.timedelta):
            return value.total_seconds()
        if not isinstance(value, numbers.Number):
            msg = "Failed to coerce value to number of seconds! (%r)"
            raise ValueError(format(msg, value))
        return value

    def round_number(count: float, keep_width: bool = False):
        text = '%.2f' % float(count)
        if not keep_width:
            text = re.sub('0+$', '', text)
            text = re.sub(r'\.$', '', text)
        return text

    def format_size(num_bytes: int, keep_width: bool = False, binary: bool = False) -> str:
        for unit in reversed(disk_size_units):
            if num_bytes >= unit.binary.divider and binary:
                number = round_number(float(num_bytes) / unit.binary.divider, keep_width=keep_width)
                return pluralize(number, unit.binary.symbol, unit.binary.symbol)
            elif num_bytes >= unit.decimal.divider and not binary:
                number = round_number(float(num_bytes) / unit.decimal.divider, keep_width=keep_width)
                return pluralize(number, unit.decimal.symbol, unit.decimal.symbol)
        return pluralize(num_bytes, 'byte')

    def format_timespan(num_seconds: Any, detailed: bool = False, max_units: int = 3) -> str:
        num_seconds = coerce_seconds(num_seconds)
        if num_seconds < 60 and not detailed:
            return pluralize(round_number(num_seconds), 'second')
        else:
            result: List[str] = []
            num_seconds = decimal.Decimal(str(num_seconds))
            relevant_units = list(reversed(time_units[0 if detailed else 3:]))
            for unit in relevant_units:
                divider = decimal.Decimal(str(unit.divider))
                count = num_seconds / divider
                num_seconds %= divider
                if unit != relevant_units[-1]:
                    count = int(count)
                else:
                    count = round_number(count)
                if count not in (0, '0'):
                    result.append(pluralize(count, unit.singular, unit.plural))
            if len(result) == 1:
                return result[0]
            else:
                if not detailed:
                    result = result[:max_units]
                return concatenate(result)


if True:  # from humanfriendly/text.py
    def format(text: str, *args: Any, **kw: Any):
        if args:
            text %= args
        if kw:
            text = text.format(**kw)
        return text

    def pluralize(count: Union[int, str], singular: str, plural: Optional[str] = None) -> str:
        return '%s %s' % (count, pluralize_raw(count, singular, plural))

    def pluralize_raw(count: Union[int, str], singular: str, plural: Optional[str] = None) -> str:
        if not plural:
            plural = singular + 's'
        return singular if float(count) == 1.0 else plural

    def concatenate(items: List[str], conjunction: str = 'and', serial_comma: bool = False) -> str:
        items = list(items)
        if len(items) > 1:
            final_item = items.pop()
            formatted = ', '.join(items)
            if serial_comma:
                formatted += ','
            return ' '.join([formatted, conjunction, final_item])
        elif items:
            return items[0]
        else:
            return ''
