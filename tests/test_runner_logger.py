import io
import logging
import sys
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch


class _FlushCountingStream(io.StringIO):

    def __init__(self):
        super().__init__()

        self.flush_count = 0

    def flush(self):
        self.flush_count += 1
        super().flush()


@unittest.skipIf(sys.platform == "win32", "the cloud runner needs the unix signals")
class CloudRunnerLoggerTest(unittest.TestCase):

    def _log(self, log_secret: str):
        from crunch.runner.cloud import _create_logger

        stdout, stderr = _FlushCountingStream(), _FlushCountingStream()

        with patch.object(sys, "stdout", stdout), patch.object(sys, "stderr", stderr):
            logger = _create_logger(log_secret)
            logger.info("started")
            logger.warning("retrying")
            logger.error("user code exited with code 1")

        return stdout, stderr

    def test_prefix(self):
        stdout, stderr = self._log("secret")

        self.assertEqual("[secret] <runner/out> started\n[secret] <runner/out> retrying\n", stdout.getvalue())
        self.assertEqual("[secret] <runner/err> user code exited with code 1\n", stderr.getvalue())

    def test_without_secret(self):
        stdout, stderr = self._log("")

        self.assertEqual("<runner/out> started\n<runner/out> retrying\n", stdout.getvalue())
        self.assertEqual("<runner/err> user code exited with code 1\n", stderr.getvalue())

    def test_flush_after_each_line(self):
        stdout, stderr = self._log("secret")

        self.assertEqual(2, stdout.flush_count)
        self.assertEqual(1, stderr.flush_count)

    def test_not_propagated_to_the_root_logger(self):
        root_logger = logging.getLogger()

        with self.assertNoLogs(root_logger):
            self._log("secret")

    def test_context_log(self):
        from crunch.runner.cloud import CloudRunnerContext, _create_logger

        stdout, stderr = _FlushCountingStream(), _FlushCountingStream()

        with patch.object(sys, "stdout", stdout), patch.object(sys, "stderr", stderr):
            runner = SimpleNamespace(logger=_create_logger("secret"))
            context = CloudRunnerContext(cast(Any, runner))

            self.assertTrue(context.log("looping", important=True))
            context.log("failed", error=True)
            context.logger.info("from the logger")

        self.assertEqual("[secret] <runner/out> looping\n[secret] <runner/out> from the logger\n", stdout.getvalue())
        self.assertEqual("[secret] <runner/err> failed\n", stderr.getvalue())

    def test_context_log_levels(self):
        from crunch.runner.cloud import CloudRunnerContext

        logger = logging.Logger("test_runner_logger")
        context = CloudRunnerContext(cast(Any, SimpleNamespace(logger=logger)))

        with self.assertLogs(logger, logging.DEBUG) as logs:
            context.log("regular")
            context.log("important", important=True)
            context.log("failed", error=True)
            context.log("failed and important", important=True, error=True)

        self.assertEqual(
            [
                "INFO:test_runner_logger:regular",
                "WARNING:test_runner_logger:important",
                "ERROR:test_runner_logger:failed",
                "ERROR:test_runner_logger:failed and important",
            ],
            logs.output,
        )
