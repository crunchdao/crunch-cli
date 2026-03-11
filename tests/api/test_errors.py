import unittest

import crunch.api as api


class ErrorsTest(unittest.TestCase):

    def test_find_error_class(self):
        self.assertEqual(
            api.CrunchNotFoundException,
            api.find_error_class("CRUNCH_NOT_FOUND")
        )

        self.assertEqual(
            api.ApiException,
            api.find_error_class("UNKNOWN_ERROR_THAT_HASNT_BEEN_DEFINED")
        )

    def test_convert_error(self):
        error = api.convert_error({
            "code": "CRUNCH_NOT_FOUND",
            "phaseType": "SUBMISSION",
            "roundNumber": 1,
            "competitionName": "datacrunch"
        })

        self.assertEqual(
            api.CrunchNotFoundException,
            type(error)
        )

        self.assertEqual(
            api.PhaseType.SUBMISSION,
            error.phase_type
        )

        self.assertEqual(
            1,
            error.round_number
        )

        self.assertEqual(
            "datacrunch",
            error.competition_name
        )
