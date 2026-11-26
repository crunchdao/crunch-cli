import os
import tempfile
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
