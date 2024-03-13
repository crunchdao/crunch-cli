import unittest

import crunch.api as api


class ClientTest(unittest.TestCase):

    client: api.Client

    @classmethod
    def setUpClass(cls):
        cls.client = api.Client.from_env()

    def test_competitions(self):
        for competition in self.client.competitions[:2]:
            print(competition.reload())

    def test_libraries(self):
        for library in self.client.libraries[:2]:
            print(library)

    def test_quickstarters(self):
        for quickstarter in self.client.quickstarters(api.CompetitionFormat.TIMESERIES)[:2]:
            print(quickstarter.reload())
            print(quickstarter.authors)
            print(quickstarter.files[0])

        adialab = self.client.competitions.get("adialab")
        for quickstarter in adialab.quickstarters[:2]:
            print(quickstarter.reload())
            print(quickstarter.authors)
            print(quickstarter.files[0])

    def test_users(self):
        for user in self.client.users[:2]:
            print(user.reload())

    def test_format_web_url(self):
        url = self.client.format_web_url("/a/b/c")

        self.assertTrue(url.startswith("http://") or url.startswith("https://"))
        self.assertTrue(url.endswith("/a/b/c"))
