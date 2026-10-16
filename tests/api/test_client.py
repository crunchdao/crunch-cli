from itertools import islice
import unittest

import crunch.api as api


class ClientTest(unittest.TestCase):

    client: api.Client

    @classmethod
    def setUpClass(cls):
        cls.client = api.Client.from_env()

    def test_competitions(self):
        for competition in islice(self.client.competitions.list(), 2):
            print(competition.reload())

    def test_libraries(self):
        for library in islice(self.client.libraries.list(), 2):
            print(library)

    def test_quickstarters(self):
        first_competition = next(islice(self.client.competitions.list(), 1))
        for quickstarter in islice(first_competition.quickstarters.list(), 2):
            print(quickstarter.reload())
            print(quickstarter.authors)
            print(quickstarter.files[0])

        adialab = self.client.competitions.get("adialab")
        for quickstarter in islice(adialab.quickstarters.list(), 2):
            print(quickstarter.reload())
            print(quickstarter.authors)
            print(quickstarter.files[0])

    def test_users(self):
        for user in islice(self.client.users.list(), 2):
            print(user.reload())

    def test_projects(self):
        adialab = self.client.competitions.get("adialab")
        for project in islice(adialab.projects.list(), 2):
            print(project)

    def test_submissions(self):
        adialab = self.client.competitions.get_reference("adialab")
        project = adialab.projects.get("@me", "@first")

        for submission in islice(project.submissions.list(), 2):
            print(submission)

    def test_runs(self):
        adialab = self.client.competitions.get_reference("adialab")
        project = adialab.projects.get("@me", "@first")

        for run in islice(project.submissions.list(), 2):
            print(run)

    def test_predictions(self):
        adialab = self.client.competitions.get_reference("adialab")
        project = adialab.projects.get("@me", "@first")

        for prediction in islice(project.predictions.list(), 2):
            print(prediction)

    def test_scores(self):
        adialab = self.client.competitions.get_reference("adialab")
        project = adialab.projects.get("@me", "@first")
        prediction = next(islice(project.predictions.list(), 1))

        for score in islice(prediction.scores.list(), 2):
            print(score)

    def test_format_web_url(self):
        url = self.client.format_web_url("/a/b/c")

        self.assertTrue(url.startswith("http://") or url.startswith("https://"))
        self.assertTrue(url.endswith("/a/b/c"))
