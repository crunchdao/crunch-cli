import unittest

import pandas
import numpy

import crunch.checker

FUNCTIONS = crunch.checker.functions
CheckError = crunch.checker.CheckError


class FunctionsTest(unittest.TestCase):

    def test_columns_name(self):
        example = pandas.DataFrame(columns=["a", "b"])
        bad = pandas.DataFrame(columns=["a", "b", "c"])
        good = example.copy()

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.columns_name(bad, example)

        self.assertEquals(
            "Columns name are different from what is expected",
            str(context.exception)
        )

        self.assertIsNone(FUNCTIONS.columns_name(good, example))

    def test_nans(self):
        nans = pandas.DataFrame([numpy.nan])
        infs = pandas.DataFrame([numpy.inf])
        infs_negative = pandas.DataFrame([-numpy.inf])
        good = pandas.DataFrame([42])

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.nans(nans)

        self.assertEquals("NaNs detected", str(context.exception))

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.nans(infs)

        self.assertEquals("inf detected", str(context.exception))

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.nans(infs_negative)

        self.assertEquals("inf detected", str(context.exception))

        self.assertIsNone(FUNCTIONS.nans(good))

    def test_values_between(self):
        column_name = "a"
        min = -42
        max = 42
        bad_more = pandas.DataFrame([max + 1], columns=[column_name])
        bad_less = pandas.DataFrame([min - 1], columns=[column_name])
        good = pandas.DataFrame([max / 2], columns=[column_name])

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.values_between(
                bad_more,
                column_name,
                min, max
            )

        self.assertEquals(
            f"Values are not between {min} and {max}",
            str(context.exception)
        )

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.values_between(
                bad_less,
                column_name,
                min, max
            )

        self.assertEquals(
            f"Values are not between {min} and {max}",
            str(context.exception)
        )

        self.assertIsNone(
            FUNCTIONS.values_between(
                good,
                column_name,
                min, max
            )
        )

    def test_values_allowed(self):
        column_name = "a"
        values = [42]
        bad = pandas.DataFrame([values[0] + 1], columns=[column_name])
        good = pandas.DataFrame([values[0]], columns=[column_name])

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.values_allowed(
                bad,
                column_name,
                values
            )

        self.assertEquals(
            f"Values should only be: {values}",
            str(context.exception)
        )

        self.assertIsNone(
            FUNCTIONS.values_allowed(
                good,
                column_name,
                values
            )
        )

    def test_moons(self):
        column_name = "a"
        moons = [24, 42]
        example = pandas.DataFrame(moons, columns=[column_name])
        bad = pandas.DataFrame(moons + [0], columns=[column_name])
        good = example.copy()

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.moons(
                bad,
                example,
                column_name
            )

        self.assertEquals(
            f"{column_name} are different from what is expected",
            str(context.exception)
        )

        self.assertIsNone(
            FUNCTIONS.moons(
                example,
                good,
                column_name
            )
        )

    def test_ids_at_moon(self):
        moon = 42
        id_column_name = "a"
        moon_column_name = "b"
        ids = [24, 42]
        example = pandas.DataFrame(ids, columns=[id_column_name])
        bad_duplicated = pandas.DataFrame(ids * 2, columns=[id_column_name])
        bad_different = pandas.DataFrame(ids + [0], columns=[id_column_name])
        good = example.copy()

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.ids_at_moon(
                bad_duplicated,
                example,
                moon,
                id_column_name,
                moon_column_name,
            )

        self.assertEquals(
            f"Duplicate ID(s) on {moon_column_name}={moon}",
            str(context.exception)
        )

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.ids_at_moon(
                bad_different,
                example,
                moon,
                id_column_name,
                moon_column_name,
            )

        self.assertEquals(
            f"Different ID(s) on {moon_column_name}={moon}",
            str(context.exception)
        )

        self.assertIsNone(
            FUNCTIONS.ids_at_moon(
                good,
                example,
                moon,
                id_column_name,
                moon_column_name,
            )
        )

    def test_constants_at_moon(self):
        moon = 42
        prediction_column_name = "a"
        moon_column_name = "b"
        ids = [24, 42]
        bad = pandas.DataFrame([ids[0]] * 2, columns=[prediction_column_name])
        good = pandas.DataFrame(ids, columns=[prediction_column_name])

        with self.assertRaises(CheckError) as context:
            FUNCTIONS.constants_at_moon(
                bad,
                moon,
                moon_column_name,
                prediction_column_name,
            )

        self.assertEquals(
            f"Constant values on {moon_column_name}={moon}",
            str(context.exception)
        )

        self.assertIsNone(
            FUNCTIONS.constants_at_moon(
                good,
                moon,
                moon_column_name,
                prediction_column_name,
            )
        )
