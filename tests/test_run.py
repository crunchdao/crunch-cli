import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from crunch.api import RunStatus
from crunch.command.run import _format_run_duration


def _run(*, status: RunStatus, started_at: datetime | None = None, duration: timedelta | None = None):
    return SimpleNamespace(status=status, started_at=started_at, duration=duration)


class FormatRunDurationTest(unittest.TestCase):

    def test_pending(self):
        self.assertIsNone(_format_run_duration(_run(status=RunStatus.PENDING)))

    def test_running(self):
        started_at = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(minutes=12, seconds=34)

        self.assertEqual("~0:12:34", _format_run_duration(_run(status=RunStatus.RUNNING, started_at=started_at)))

    def test_completed(self):
        started_at = datetime(2026, 11, 20, 15, 30, 17)

        self.assertEqual("0:42:00", _format_run_duration(_run(status=RunStatus.COMPLETED, started_at=started_at, duration=timedelta(minutes=42))))

    def test_completed_without_duration(self):
        started_at = datetime(2026, 11, 20, 15, 30, 17)

        self.assertIsNone(_format_run_duration(_run(status=RunStatus.COMPLETED, started_at=started_at, duration=timedelta())))
