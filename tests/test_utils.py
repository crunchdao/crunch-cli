import unittest

import requests

from crunch.utils import cut_url, download


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


class DownloadTest(unittest.TestCase):

    def test_regular(self):

        with self.assertRaises(requests.HTTPError) as ctx:
            download(
                "https://crunchdao--competition--production.s3.amazonaws.com/file?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20260823T205142Z&X-Amz-SignedHeaders=host&X-Amz-Credential=AAAA&X-Amz-Expires=7200&X-Amz-Signature=BBBB",
                "/dev/null",
            )

        message = str(ctx.exception)
        self.assertEqual("403 Client Error: Forbidden for url: https://crunchdao--competition--production.s3.amazonaws.com/file?X-Amz-Algorithm=(hidden)&X-Amz-Date=(hidden)&X-Amz-SignedHeaders=(hidden)&X-Amz-Credential=(hidden)&X-Amz-Expires=(hidden)&X-Amz-Signature=(hidden)", message)
