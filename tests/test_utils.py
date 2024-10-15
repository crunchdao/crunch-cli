import unittest

from crunch.utils import cut_url, is_valid_version, pip_freeze


class CutUrlTest(unittest.TestCase):

    def test_regular(self):
        self.assertEqual("http:google.com/search", cut_url("http://google.com/search"))
        self.assertEqual("https:google.com/search", cut_url("https://google.com/search"))

    def test_remove_query(self):
        self.assertEqual("http:google.com/search", cut_url("http://google.com/search?q=hello"))
        self.assertEqual("https:google.com/search", cut_url("https://google.com/search?q=hello"))

    def test_keep_double(self):
        self.assertEqual("http:google.com//search//a", cut_url("http://google.com//search//a?q=hello"))
        self.assertEqual("https:google.com//search//a", cut_url("https://google.com//search//a?q=hello"))


class PipTest(unittest.TestCase):

    def test_is_valid_version(self):
        self.assertTrue(is_valid_version("1.2.3"))
        self.assertFalse(is_valid_version("a1"))

    def test_pip_freeze(self):
        working_set = pip_freeze()

        self.assertTrue(len(working_set) != 0)
        self.assertIn("crunch-cli", working_set)
