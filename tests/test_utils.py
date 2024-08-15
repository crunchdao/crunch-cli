import unittest

from crunch.utils import cut_url


class CutUrlTest(unittest.TestCase):

    def test_regular(self):
        self.assertEqual("//google.com", cut_url("https://google.com"))

    def test_remove_query(self):
        self.assertEqual("//google.com", cut_url("https://google.com?q=hello"))
