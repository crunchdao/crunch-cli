import unittest

from crunch.utils import cut_url


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
