import unittest

from crunch.api import Competition, Metric, ScorerFunction, Target
from crunch.meta import filter_metrics, to_column_name


class ColumnNameTest(unittest.TestCase):

    def test_to_column_name(self):
        competition = Competition()

        metric = Metric(competition, attrs={
            "target": {},
            "name": "hello",
        })

        self.assertEqual(
            "time$$meta$$hello",
            to_column_name(metric, "time")
        )


class FilterTest(unittest.TestCase):

    def test_filter_metrics(self):
        competition = Competition()

        metric = Metric(competition, attrs={
            "target": {
                "name": "red"
            },
            "name": "hello",
            "scorerFunction": ScorerFunction.SPEARMAN.name,
        })

        self.assertEqual(
            [metric],
            filter_metrics([metric], "red", ScorerFunction.SPEARMAN)
        )

        self.assertEqual(
            [],
            filter_metrics([metric], "blue", ScorerFunction.SPEARMAN)
        )

        self.assertEqual(
            [metric],
            filter_metrics([metric], None, ScorerFunction.SPEARMAN)
        )

        self.assertEqual(
            [],
            filter_metrics([metric], None, ScorerFunction.RANDOM)
        )

        self.assertEqual(
            [],
            filter_metrics([metric], "blue", ScorerFunction.RANDOM)
        )
