import unittest
from unittest.mock import MagicMock

from crunch.api._domain.runtime import RuntimeOptionCollection


def _make_project() -> MagicMock:
    project = MagicMock()
    project.competition.id = 1
    project.user_id = 2
    project.name = "project"
    project.client.api.paginate_json.return_value = iter([])

    return project


class RuntimeOptionCollectionTest(unittest.TestCase):

    def test_without_submission(self):
        project = _make_project()

        list(RuntimeOptionCollection(project).iter())

        project.client.api.paginate_json.assert_called_once_with(
            ["v3", "competitions", 1, "projects", 2, "project", "runtime-options"],
            params={},
            page_size=1000,
        )

    def test_with_submission(self):
        project = _make_project()

        submission = MagicMock()
        submission.number = 3

        list(RuntimeOptionCollection(project, submission).iter())

        project.client.api.paginate_json.assert_called_once_with(
            ["v3", "competitions", 1, "projects", 2, "project", "runtime-options"],
            params={"submissionNumber": 3},
            page_size=1000,
        )
