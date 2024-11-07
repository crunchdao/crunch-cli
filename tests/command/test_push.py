import unittest

from crunch.command.push import is_valid_version, pip_freeze


class PipTest(unittest.TestCase):

    def test_is_valid_version(self):
        self.assertTrue(is_valid_version("1.2.3"))
        self.assertFalse(is_valid_version("a1"))

    def test_pip_freeze(self):
        working_set = pip_freeze()

        self.assertTrue(len(working_set) != 0)
        self.assertIn("crunch-cli", working_set)
