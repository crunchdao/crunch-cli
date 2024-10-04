import collections
import unittest

from crunch.api import ColumnNames, DataReleaseFeature, TargetColumnNames
from crunch.container import Columns, Features, GeneratorWrapper, StreamMessage


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
            id="",
            moon="",
            side=None,
            input=None,
            output=None,
            targets=[
                TargetColumnNames(0, "a", "side_a", "in_a", "out_a"),
                TargetColumnNames(0, "b", "side_b", "in_b", "out_b")
            ]
        )

        inputs, outputs = Columns.from_model(column_names)

        self.assertEqual("{a: 'in_a', b: 'in_b'}", repr(inputs))
        self.assertEqual("{a: 'out_a', b: 'out_b'}", repr(outputs))

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


class GeneratorWrapperTest(unittest.TestCase):

    def test_yield_not_called_before_loop(self):
        def consumer(stream: iter):
            next(stream)

        with self.assertRaises(RuntimeError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer
            )

        self.assertEqual(GeneratorWrapper.ERROR_YIELD_MUST_BE_CALLED_BEFORE, str(context.exception))

    def test_previous_value_not_yield(self):
        def consumer(stream: iter):
            yield

            next(stream)
            next(stream)

        with self.assertRaises(RuntimeError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer
            ).collect(3)

        self.assertEqual(GeneratorWrapper.ERROR_PREVIOUS_VALUE_NOT_YIELD, str(context.exception))

    def test_no_yield(self):
        def consumer(stream: iter):
            pass

        with self.assertRaises(RuntimeError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer
            )

        self.assertEqual(GeneratorWrapper.ERROR_YIELD_NOT_CALLED, str(context.exception))

    def test_first_yield_is_none(self):
        def consumer(stream: iter):
            yield 42

        with self.assertRaises(ValueError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer
            )

        self.assertEqual(GeneratorWrapper.ERROR_FIRST_YIELD_MUST_BE_NONE, str(context.exception))

    def test_multiple_yield(self):
        def consumer(stream: iter):
            yield

            next(stream)
            yield 42
            yield 21

        with self.assertRaises(RuntimeError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer
            ).collect(3)

        self.assertEqual(GeneratorWrapper.ERROR_MULTIPLE_YIELD, str(context.exception))

    def test_missing_yields(self):
        if True:
            def consumer(stream: iter):
                yield

                next(stream)
                yield 42

                next(stream)
                yield 21

            with self.assertRaises(ValueError) as context:
                GeneratorWrapper(
                    iter([1, 2, 3]),
                    consumer
                ).collect(3)

        self.assertTrue(f"{GeneratorWrapper.ERROR_MULTIPLE_YIELD} (2 / 3)", str(context.exception))

        if True:
            def consumer(stream: iter):
                yield

                next(stream)
                yield 42

                next(stream)
                yield 21

                next(stream)

            with self.assertRaises(ValueError) as context:
                GeneratorWrapper(
                    iter([1, 2, 3]),
                    consumer
                ).collect(3)

            self.assertTrue(f"{GeneratorWrapper.ERROR_MULTIPLE_YIELD} (2 / 3)", str(context.exception))

    def test_working(self):
        def consumer(stream: iter):
            yield

            for message in stream:
                yield message.x * 2

        collected = GeneratorWrapper(
            iter([1, 2, 3]),
            consumer
        ).collect(3)

        self.assertListEqual([2, 4, 6], collected)


class StreamMessageTest(unittest.TestCase):

    def test_get_item(self):
        message = StreamMessage(42)

        self.assertEqual(42, message["x"])

        with self.assertRaises(AttributeError) as context:
            message["a"]

        self.assertEqual("'StreamMessage' object has no attribute 'a'", str(context.exception))

    def test_get_attr(self):
        message = StreamMessage(42)

        self.assertEqual(42, message.x)

    def test_set_item_x(self):
        message = StreamMessage(42)

        with self.assertRaises(AttributeError) as context:
            message["x"] = 21

        self.assertEqual("Cannot set key 'x' - object is immutable", str(context.exception))

    def test_set_attr_x(self):
        message = StreamMessage(42)

        with self.assertRaises(AttributeError) as context:
            message.x = 21

        self.assertEqual("Cannot set attribute 'x' - object is immutable", str(context.exception))

    def test_set_item_other(self):
        message = StreamMessage(42)

        with self.assertRaises(AttributeError) as context:
            message["a"] = 21

        self.assertEqual("Cannot set key 'a' - object is immutable", str(context.exception))

    def test_set_attr_other(self):
        message = StreamMessage(42)

        with self.assertRaises(AttributeError) as context:
            message.a = 21

        self.assertEqual("Cannot set attribute 'a' - object is immutable", str(context.exception))

    def test_iter(self):
        message = StreamMessage(42)

        self.assertEqual(["x"], list(message))

        iterator = iter(message)
        self.assertEqual("x", next(iterator))
        self.assertEqual(None, next(iterator, None))

    def test_repr(self):
        message = StreamMessage(42)

        self.assertEqual("{x: 42}", repr(message))

    def test_str(self):
        message = StreamMessage(42)

        self.assertEqual("StreamMessage({x: 42})", str(message))

    def test_get(self):
        message = StreamMessage(42)

        self.assertEqual("StreamMessage({x: 42})", str(message))

    def test_keys(self):
        message = StreamMessage(42)

        self.assertEqual({"x"}, message.keys())

    def test_values(self):
        message = StreamMessage(42)

        self.assertEqual([42], list(message.values()))

    def test_items(self):
        message = StreamMessage(42)

        self.assertEqual([("x", 42)], list(message.items()))
