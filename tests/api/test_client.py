import unittest

import crunch.api as api


class ClientTest(unittest.TestCase):

    client: api.Client

    @classmethod
    def setUpClass(cls):
        cls.client = api.Client.from_env()

    def test_competitions(self):
        for competition in self.client.competitions[:5]:
            print(competition)

    def test_libraries(self):
        for library in self.client.libraries[:5]:
            print(library)

    def test_users(self):
        for user in self.client.users[:5]:
            print(user)

    def test_format_web_url(self):
        url = self.client.format_web_url("/a/b/c")

        self.assertTrue(url.startswith("http://") or url.startswith("https://"))
        self.assertTrue(url.endswith("/a/b/c"))
