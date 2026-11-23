import logging
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import click
import requests

from crunch import library


class ScanTest(unittest.TestCase):

    def test_server_error_is_not_reported_as_forbidden(self):
        whitelist = MagicMock()
        whitelist.find_library.side_effect = requests.HTTPError("502 Server Error: Bad Gateway")

        logger = logging.getLogger("test_library")

        with tempfile.TemporaryDirectory() as directory:
            requirements_file = os.path.join(directory, "requirements.txt")
            with open(requirements_file, "w") as fd:
                fd.write("numpy\npandas\n")

            with patch.object(library, "CrunchHubWhitelist", return_value=whitelist), self.assertLogs(logger) as logs:
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
