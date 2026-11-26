import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest


class ReadSecretTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.file_path = os.path.join(self._directory.name, "secret")

        with open(self.file_path, "w") as fd:
            fd.write("from-file")

    def tearDown(self):
        self._directory.cleanup()

    def test_file_deleted_once_read(self):
        from crunch.runner.cloud import read_secret

        self.assertEqual("from-file", read_secret("secret", None, self.file_path))
        self.assertFalse(os.path.exists(self.file_path))

    def test_value(self):
        from crunch.runner.cloud import read_secret

        self.assertEqual("from-value", read_secret("secret", "from-value", None))

    def test_both_given(self):
        from crunch.runner.cloud import read_secret

        with self.assertRaisesRegex(ValueError, "both"):
            read_secret("secret", "from-value", self.file_path)

    def test_empty_file(self):
        from crunch.runner.cloud import read_secret

        with open(self.file_path, "w"):
            pass

        with self.assertRaisesRegex(ValueError, "no secret"):
            read_secret("secret", None, self.file_path)

    def test_nothing_given(self):
        from crunch.runner.cloud import read_secret

        with self.assertRaisesRegex(ValueError, "no secret"):
            read_secret("secret", None, None)


@unittest.skipIf(sys.platform == "win32", "the cloud runner only runs on linux")
class CollectZombieChildrenTest(unittest.TestCase):

    def test_collected(self):
        from crunch.runner.cloud import _collect_zombie_children

        ended_pid = os.fork()
        if ended_pid == 0:
            os._exit(0)

        alive_pid = os.fork()
        if alive_pid == 0:
            time.sleep(10)
            os._exit(0)

        try:
            # until its parent collects it, an ended child stays a zombie
            for _ in range(100):
                with open(f"/proc/{ended_pid}/stat") as fd:
                    if fd.read().rsplit(")", 1)[1].split()[0] == "Z":
                        break

                time.sleep(0.01)

            _collect_zombie_children()

            with self.assertRaises(ChildProcessError):
                os.waitpid(ended_pid, os.WNOHANG)

            # still running, not waited for
            self.assertEqual((0, 0), os.waitpid(alive_pid, os.WNOHANG))
        finally:
            os.kill(alive_pid, signal.SIGKILL)
            os.waitpid(alive_pid, 0)


@unittest.skipIf(sys.platform == "win32", "the cloud runner only runs on linux")
class SetupTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.bin_directory = self._directory.name

        for command in ["sandbox", "prefix"]:
            self._create_command(command)

    def tearDown(self):
        self._directory.cleanup()

    def _create_command(self, command: str):
        path = os.path.join(self.bin_directory, command)
        with open(path, "w") as fd:
            fd.write("#!/bin/sh\n")

        os.chmod(path, 0o755)

    def _run_setup(self, timeout: int | None):
        """
        Only `setup` of a runner, which then prints the commands it located and sleeps.
        """

        return subprocess.Popen(
            [
                sys.executable, "-c",
                "import time\n"
                "from crunch.runner.cloud import CloudRunner, _create_logger\n"
                "runner = CloudRunner.__new__(CloudRunner)\n"
                f"runner.timeout = {timeout}\n"
                "runner.logger = _create_logger('secret')\n"
                "runner.setup()\n"
                "print(runner.sandbox_path, runner.prefix_path, flush=True)\n"
                "time.sleep(30)\n",
            ],
            env={**os.environ, "PATH": self.bin_directory},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def test_commands_located(self):
        process = self._run_setup(None)

        self.assertEqual(f"{self.bin_directory}/sandbox {self.bin_directory}/prefix\n", process.stdout.readline())

        process.kill()
        process.communicate(timeout=10)

    def test_command_missing(self):
        os.unlink(os.path.join(self.bin_directory, "prefix"))

        process = self._run_setup(None)

        _, stderr = process.communicate(timeout=10)
        self.assertEqual(1, process.returncode, stderr)
        self.assertIn("command not found: prefix", stderr)
