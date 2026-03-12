# https://pypi.org/project/humanfriendly/

import collections
import re
from typing import Optional

if True:  # from humanfriendly/__init__.py
    SizeUnit = collections.namedtuple('SizeUnit', 'divider, symbol, name')
    CombinedUnit = collections.namedtuple('CombinedUnit', 'decimal, binary')

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

    def coerce_boolean(value):
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

    def round_number(count, keep_width=False):
        text = '%.2f' % float(count)
        if not keep_width:
            text = re.sub('0+$', '', text)
            text = re.sub(r'\.$', '', text)
        return text


if True:  # from humanfriendly/text.py
    def format(text: str, *args, **kw):
        if args:
            text %= args
        if kw:
            text = text.format(**kw)
        return text

    def pluralize(count: int, singular: str, plural: Optional[str] = None) -> str:
        return '%s %s' % (count, pluralize_raw(count, singular, plural))

    def format_size(num_bytes: int, keep_width: bool = False, binary: bool = False) -> str:
        for unit in reversed(disk_size_units):
            if num_bytes >= unit.binary.divider and binary:
                number = round_number(float(num_bytes) / unit.binary.divider, keep_width=keep_width)
                return pluralize(number, unit.binary.symbol, unit.binary.symbol)
            elif num_bytes >= unit.decimal.divider and not binary:
                number = round_number(float(num_bytes) / unit.decimal.divider, keep_width=keep_width)
                return pluralize(number, unit.decimal.symbol, unit.decimal.symbol)
        return pluralize(num_bytes, 'byte')

    def pluralize_raw(count: float, singular: str, plural: Optional[str] = None) -> str:
        if not plural:
            plural = singular + 's'
        return singular if float(count) == 1.0 else plural
