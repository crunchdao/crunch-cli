import unittest

import crunch.api as api
import crunch.api.errors as errors


class ErrorsTest(unittest.TestCase):

    def test_find_error_class(self):
        self.assertEqual(
            errors.CrunchNotFoundException,
            errors.find_error_class("CRUNCH_NOT_FOUND")
        )

        self.assertEqual(
            errors.ApiException,
            errors.find_error_class("UNKNOWN_ERROR_THAT_HASNT_BEEN_DEFINED")
        )

    def test_convert_error(self):
        error = errors.convert_error({
            "code": "CRUNCH_NOT_FOUND",
            "phaseType": "SUBMISSION",
            "roundNumber": 1,
            "competitionName": "datacrunch"
        })

        self.assertEqual(
            errors.CrunchNotFoundException,
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
