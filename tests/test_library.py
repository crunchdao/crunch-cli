import logging
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import click
import requests
from crunch_convert import RequirementLanguage
from crunch_convert.requirements_txt import Library, MultipleLibraryAliasCandidateException, Whitelist

from crunch import library
from crunch.library import ImportLocation


class ScanTest(unittest.TestCase):

    def test_server_error_is_not_reported_as_forbidden(self):
        whitelist = MagicMock()
        whitelist.find_library.side_effect = requests.HTTPError("502 Server Error: Bad Gateway")

        logger = logging.getLogger("test_library")

        with tempfile.TemporaryDirectory() as directory:
            requirements_file = os.path.join(directory, "requirements.txt")
            with open(requirements_file, "w") as fd:
                fd.write("numpy\npandas\n")

            with patch("crunch.library._scan.CrunchHubWhitelist", return_value=whitelist), self.assertLogs(logger) as logs:
                library.scan(
                    requirements_file=requirements_file,
                    requirements_r_file=os.path.join(directory, "requirements.r.txt"),
                    logger=logger,
                )

        self.assertEqual(["WARNING:test_library:whitelist could not be reached, requirements not checked: 502 Server Error: Bad Gateway"], logs.output)

    def test_parse_error_is_logged(self):
        logger = logging.getLogger("test_library")

        with tempfile.TemporaryDirectory() as directory:
            requirements_file = os.path.join(directory, "requirements.txt")
            with open(requirements_file, "w") as fd:
                fd.write("numpy==\n")

            with self.assertRaises(click.Abort), self.assertLogs(logger) as logs:
                library.scan(
                    requirements_file=requirements_file,
                    requirements_r_file=os.path.join(directory, "requirements.r.txt"),
                    logger=logger,
                )

        self.assertEqual(1, len(logs.records))
        self.assertEqual(logging.ERROR, logs.records[0].levelno)
        self.assertTrue(logs.records[0].getMessage().startswith("requirements.txt: invalid syntax"))


class _FakeWhitelist(Whitelist):

    def __init__(self, names_by_alias: dict[str, list[str]] = {}, *, standard_aliases: set[str] = set(), unreachable: bool = False):
        super().__init__()

        self._names_by_alias = names_by_alias
        self._standard_aliases = standard_aliases
        self._unreachable = unreachable

        self.aliases: list[str] = []

    def find_library(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        *,
        language: RequirementLanguage = RequirementLanguage.PYTHON,
        name: str | None = None,
        alias: str | None = None,
    ) -> Library | None:
        assert alias is not None
        self.aliases.append(alias)

        if self._unreachable:
            raise requests.ConnectionError("unreachable")

        if alias in self._standard_aliases:
            return Library(language=language, name=alias, aliases=[alias], standard=True, freeze=False)

        names = self._names_by_alias.get(alias, [])
        if len(names) > 1:
            raise MultipleLibraryAliasCandidateException(alias=alias, names=set(names))

        if not names:
            return None

        return Library(language=language, name=names[0], aliases=[alias], standard=False, freeze=True)


_LOCATION = ImportLocation(file_path="main.py", line=1)


class FindImportProblemsTest(unittest.TestCase):

    def _find(self, aliases: list[str], whitelist: Whitelist, requirement_names: list[str] = []):
        problems = library.find_import_problems(
            imports={alias: _LOCATION for alias in aliases},
            requirement_names=requirement_names,
            whitelist=whitelist,
        )

        return {problem.alias: problem.names for problem in problems}

    def test_not_listed(self):
        self.assertEqual({"sklearn": ["scikit-learn"]}, self._find(["sklearn"], _FakeWhitelist({"sklearn": ["scikit-learn"]})))

    def test_listed_under_the_import_name(self):
        whitelist = _FakeWhitelist()

        self.assertEqual({}, self._find(["catboost"], whitelist, ["CatBoost"]))
        self.assertEqual([], whitelist.aliases)

    def test_listed_under_the_library_name(self):
        self.assertEqual({}, self._find(["sklearn"], _FakeWhitelist({"sklearn": ["scikit-learn"]}), ["Scikit-Learn"]))

    def test_not_whitelisted(self):
        self.assertEqual({"abcdef": []}, self._find(["abcdef"], _FakeWhitelist()))

    def test_ambiguous_import_name(self):
        whitelist = _FakeWhitelist({"cv2": ["opencv-python", "opencv-contrib-python"]})

        self.assertEqual({"cv2": ["opencv-contrib-python", "opencv-python"]}, self._find(["cv2"], whitelist))
        self.assertEqual({}, self._find(["cv2"], whitelist, ["opencv-python"]))

    def test_standard_in_the_whitelist(self):
        self.assertEqual({}, self._find(["six"], _FakeWhitelist(standard_aliases={"six"})))

    def test_crunch_and_standard_library(self):
        whitelist = _FakeWhitelist()

        self.assertEqual({}, self._find(["crunch", "json", "os"], whitelist))
        self.assertEqual([], whitelist.aliases)


class ScanImportsTest(unittest.TestCase):

    def _scan(self, aliases: list[str], whitelist: Whitelist, requirements_txt: str = ""):
        logger = logging.Logger("test_library")

        with tempfile.TemporaryDirectory() as directory:
            with open(os.path.join(directory, "requirements.txt"), "w") as fd:
                fd.write(requirements_txt)

            with self.assertLogs(logger) as logs:
                library.scan_imports(
                    imports={
                        alias: ImportLocation(file_path=os.path.join(directory, "main.py"), line=line)
                        for line, alias in enumerate(aliases, start=1)
                    },
                    project_directory_path=directory,
                    competition_name="structural-break",
                    logger=logger,
                    whitelist=whitelist,
                )

        return logs.output

    def test_each_problem(self):
        output = self._scan(
            ["numpy", "sklearn", "cv2", "abcdef"],
            _FakeWhitelist({"sklearn": ["scikit-learn"], "cv2": ["opencv-python", "opencv-contrib-python"]}),
            "numpy==2.0.0\n",
        )

        self.assertEqual(
            [
                "WARNING:test_library:missing requirement: scikit-learn is imported as `sklearn` by main.py:2 but is not in requirements.txt, the cloud run might fail",
                "WARNING:test_library:missing requirement: opencv-contrib-python or opencv-python is imported as `cv2` by main.py:3 but is not in requirements.txt, the cloud run might fail",
                "ERROR:test_library:forbidden library: `abcdef` is imported by main.py:4 but is not whitelisted, the cloud run might fail",
            ],
            output[:3],
        )
        self.assertRegex(output[3], r"^ERROR:test_library:  -> request to whitelist: \S+/competitions/structural-break/resources/whitelisted-libraries\?requestAlias=abcdef&requestLanguage=PYTHON$")
        self.assertEqual(4, len(output))

    def test_same_name(self):
        output = self._scan(["catboost"], _FakeWhitelist({"catboost": ["catboost"]}))

        self.assertEqual(["WARNING:test_library:missing requirement: catboost is imported by main.py:1 but is not in requirements.txt, the cloud run might fail"], output)

    def test_whitelist_unreachable(self):
        output = self._scan(["catboost"], _FakeWhitelist(unreachable=True))

        self.assertEqual(["WARNING:test_library:whitelist could not be reached, the imports are not checked: unreachable"], output)
