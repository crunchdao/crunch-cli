import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest


@unittest.skipIf(sys.platform == "win32", "the cloud runner only runs on linux")
class FindFilesTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.root = os.path.join(self._directory.name, "model")
        self.outside = os.path.join(self._directory.name, "data")
        os.makedirs(self.root)
        os.makedirs(self.outside)

        self.hidden_file = os.path.join(self.outside, "hidden.parquet")
        with open(self.hidden_file, "w") as fd:
            fd.write("hidden")

    def tearDown(self):
        self._directory.cleanup()

    def _write(self, name: str):
        with open(os.path.join(self.root, name), "w") as fd:
            fd.write("x")

    def _find(self):
        from crunch.runner.cloud import _find_files

        return sorted((name, reason) for _, name, reason in _find_files(self.root))

    def test_regular_files(self):
        self._write("model.pkl")
        os.makedirs(os.path.join(self.root, "sub"))
        self._write(os.path.join("sub", "weights.bin"))

        self.assertEqual([("model.pkl", None), (os.path.join("sub", "weights.bin"), None)], self._find())

    def test_symbolic_link_to_a_file(self):
        os.symlink(self.hidden_file, os.path.join(self.root, "leak.parquet"))

        self.assertEqual([("leak.parquet", "is a symbolic link")], self._find())

    def test_symbolic_link_to_a_directory(self):
        os.symlink(self.outside, os.path.join(self.root, "data"))

        self.assertEqual([("data", "is a symbolic link")], self._find())

    def test_hard_link(self):
        os.link(self.hidden_file, os.path.join(self.root, "leak.parquet"))

        self.assertEqual([("leak.parquet", "is a hard link")], self._find())

    def test_fifo(self):
        os.mkfifo(os.path.join(self.root, "pipe"))

        self.assertEqual([("pipe", "is not a regular file")], self._find())


class ReadEndTest(unittest.TestCase):

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.file_path = os.path.join(self._directory.name, "trace.txt")

    def tearDown(self):
        self._directory.cleanup()

    def _write(self, content: bytes):
        with open(self.file_path, "wb") as fd:
            fd.write(content)

    def test_short(self):
        from crunch.runner.cloud import _read_end

        self._write(b"Traceback")

        self.assertEqual("Traceback", _read_end(self.file_path, 16))

    def test_empty(self):
        from crunch.runner.cloud import _read_end

        self._write(b"")

        self.assertEqual("", _read_end(self.file_path, 16))

    def test_cut_to_the_end(self):
        from crunch.runner.cloud import _read_end

        self._write(b"x" * 100 + b"ValueError: bad")

        self.assertEqual("(cut to its last 15 bytes)\nValueError: bad", _read_end(self.file_path, 15))

    def test_invalid_utf8(self):
        from crunch.runner.cloud import _read_end

        self._write(b"\xff\xfeerror")

        replacement_character = "\N{REPLACEMENT CHARACTER}"
        self.assertEqual(f"{replacement_character * 2}error", _read_end(self.file_path, 16))


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

    def test_sigterm(self):
        process = self._run_setup(None)
        process.stdout.readline()

        process.send_signal(signal.SIGTERM)

        _, stderr = process.communicate(timeout=10)
        self.assertEqual(128 + signal.SIGTERM, process.returncode, stderr)

    def test_timeout_logged_with_the_secret(self):
        process = self._run_setup(1)

        _, stderr = process.communicate(timeout=10)
        self.assertEqual(124, process.returncode, stderr)
        self.assertIn("[secret] <runner/err> timeout of 1 seconds reached", stderr)
