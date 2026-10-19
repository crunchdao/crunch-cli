import unittest

import crunch.api as api


class WebUrlFormatterTest(unittest.TestCase):

    formatter = api.WebUrlFormatter("https://hub.crunchdao.com/")

    def test_home(self):
        self.assertEqual("https://hub.crunchdao.com/", self.formatter.home())

    def test_account(self):
        self.assertEqual("https://hub.crunchdao.com/account", self.formatter.account())

    def test_competition(self):
        self.assertEqual("https://hub.crunchdao.com/competitions/adialab", self.formatter.competition("adialab"))

    def test_submit(self):
        self.assertEqual("https://hub.crunchdao.com/competitions/adialab/submit", self.formatter.submit("adialab"))

    def test_submit_notebook(self):
        self.assertEqual("https://hub.crunchdao.com/competitions/adialab/submit/notebook", self.formatter.submit_notebook("adialab"))

        self.assertEqual(
            "https://hub.crunchdao.com/competitions/adialab/submit/notebook?projectName=default&message=hello+world%26more",
            self.formatter.submit_notebook("adialab", project_name="default", message="hello world&more"),
        )

    def test_whitelisted_libraries(self):
        self.assertEqual(
            "https://hub.crunchdao.com/competitions/adialab/resources/whitelisted-libraries?requestName=torch&requestLanguage=PYTHON",
            self.formatter.whitelisted_libraries("adialab", request_name="torch", request_language="PYTHON"),
        )

        self.assertEqual(
            "https://hub.crunchdao.com/competitions/adialab/resources/whitelisted-libraries?requestAlias=sklearn",
            self.formatter.whitelisted_libraries("adialab", request_alias="sklearn"),
        )

    def test_submission(self):
        self.assertEqual(
            "https://hub.crunchdao.com/competitions/adialab/projects/42/default/submissions/7",
            self.formatter.submission("adialab", user_id=42, project_name="default", submission_number=7),
        )

    def test_create_run(self):
        self.assertEqual(
            "https://hub.crunchdao.com/competitions/adialab/models/enzo/default/runs/create?submissionNumber=7",
            self.formatter.create_run("adialab", user_login="enzo", project_name="default", submission_number=7),
        )

    def test_deploy_submission(self):
        self.assertEqual(
            "https://hub.crunchdao.com/competitions/adialab/models/enzo/default/submissions/7?openDeploy-123=true",
            self.formatter.deploy_submission("adialab", user_login="enzo", project_name="default", submission_number=7, submission_id=123),
        )

    def test_base_url_without_trailing_slash(self):
        formatter = api.WebUrlFormatter("http://localhost:3000")

        self.assertEqual("http://localhost:3000/competitions/adialab", formatter.competition("adialab"))

    def test_segments_are_quoted(self):
        self.assertEqual("https://hub.crunchdao.com/competitions/a%2Fb", self.formatter.competition("a/b"))
