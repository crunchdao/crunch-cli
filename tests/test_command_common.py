import unittest
from datetime import datetime, timedelta, timezone

from crunch.command._common import reformat_datetime


class ReformatDatetimeTest(unittest.TestCase):

    def test_assumes_utc_for_naive_datetime(self):
        input = datetime(2026, 1, 2, 12, 30)
        expected = input.replace(tzinfo=timezone.utc).astimezone().replace(tzinfo=None)

        self.assertEqual(expected.isoformat(sep=" "), reformat_datetime(input))

    def test_preserves_offset_for_aware_datetime(self):
        input = datetime(2026, 1, 2, 12, 30, tzinfo=timezone(timedelta(hours=2)))
        expected = input.astimezone().replace(tzinfo=None)

        self.assertEqual(expected.isoformat(sep=" "), reformat_datetime(input))
