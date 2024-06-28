import collections
import unittest

from crunch.api import ColumnNames, DataReleaseFeature, TargetColumnNames
from crunch.container import Columns, Features


class ColumnsTest(unittest.TestCase):

    def test_get_item(self):
        columns = Columns(collections.OrderedDict(a="b"))

        self.assertEqual("b", columns["a"])

    def test_get_attr(self):
        columns = Columns(collections.OrderedDict(a="b"))

        self.assertEqual("b", columns.a)

    def test_set_item(self):
        columns = Columns(collections.OrderedDict(a="b"))

        with self.assertRaises(AttributeError) as context:
            columns["a"] = "c"

        self.assertEqual("Cannot set key 'a' - object is immutable", str(context.exception))

    def test_set_attr(self):
        columns = Columns(collections.OrderedDict(a="b"))

        with self.assertRaises(AttributeError) as context:
            columns.a = "c"

        self.assertEqual("Cannot set attribute 'a' - object is immutable", str(context.exception))

    def test_iter(self):
        columns = Columns(collections.OrderedDict(a="b", c="d"))

        self.assertEqual(["b", "d"], list(columns))

        iterator = iter(columns)
        self.assertEqual("b", next(iterator))
        self.assertEqual("d", next(iterator))
        self.assertEqual(None, next(iterator, None))

    def test_repr(self):
        columns = Columns(collections.OrderedDict(a="b", c="d"))

        self.assertEqual("{a: 'b', c: 'd'}", repr(columns))

    def test_str(self):
        columns = Columns(collections.OrderedDict(a="b", c="d"))

        self.assertEqual("Columns({a: 'b', c: 'd'})", str(columns))

    def test_from_model(self):
        column_names = ColumnNames(
            id=None,
            moon=None,
            targets=[
                TargetColumnNames(None, "a", "b", "c"),
                TargetColumnNames(None, "d", "e", "f")
            ]
        )

        inputs, outputs = Columns.from_model(column_names)

        self.assertEqual("{a: 'b', d: 'e'}", repr(inputs))
        self.assertEqual("{a: 'c', d: 'f'}", repr(outputs))

    def test_no_external_access(self):
        columns = Columns(collections.OrderedDict(a="b"))

        with self.assertRaises(AttributeError) as context:
            columns._storage

        self.assertEqual("'Columns' object has no attribute '_storage'", str(context.exception))


class FeaturesTest(unittest.TestCase):

    def test_to_parameter_variants(self):
        features = Features(
            [
                DataReleaseFeature("v1", "hello"),
                DataReleaseFeature("v1", "world"),
                DataReleaseFeature("v2", "from"),
                DataReleaseFeature("v2", "python"),
            ],
            default_group_name="v2"
        )

        base_name = "feature_column_names"
        separator = "_"

        self.assertEqual(
            {
                base_name: ["from", "python"],
                f"{base_name}{separator}v1": ["hello", "world"],
                f"{base_name}{separator}v2": ["from", "python"],
            },
            features.to_parameter_variants(base_name, separator)
        )

    def test_to_parameter_variants_duplicates(self):
        features = Features(
            [
                DataReleaseFeature("v1", "hello"),
                DataReleaseFeature("v1", "world"),
                DataReleaseFeature("v1", "hello"),
            ],
            default_group_name="v1"
        )

        base_name = "feature_column_names"
        separator = "_"

        self.assertEqual(
            {
                base_name: ["hello", "world"],
                f"{base_name}{separator}v1": ["hello", "world"],
            },
            features.to_parameter_variants(base_name, separator)
        )

    def test_to_parameter_variants_invalid_default(self):
        features = Features(
            [
                DataReleaseFeature("v1", "hello"),
                DataReleaseFeature("v1", "world"),
            ],
            default_group_name="v2"
        )

        base_name = "feature_column_names"
        separator = "_"

        self.assertEqual(
            {
                base_name: [],
                f"{base_name}{separator}v1": ["hello", "world"],
                f"{base_name}{separator}v2": [],
            },
            features.to_parameter_variants(base_name, separator)
        )
