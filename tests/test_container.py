import unittest

from crunch.container import GeneratorWrapper


class GeneratorWrapperTest(unittest.TestCase):

    def test_yield_not_called_before_loop(self):
        def consumer(stream: iter):
            next(stream)

        with self.assertRaises(RuntimeError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer,
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
                consumer,
            ).collect(3)

        self.assertEqual(GeneratorWrapper.ERROR_PREVIOUS_VALUE_NOT_YIELD, str(context.exception))

    def test_no_yield(self):
        def consumer(stream: iter):
            pass

        with self.assertRaises(RuntimeError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer,
            )

        self.assertEqual(GeneratorWrapper.ERROR_YIELD_NOT_CALLED, str(context.exception))

    def test_first_yield_is_none(self):
        def consumer(stream: iter):
            yield 42

        with self.assertRaises(ValueError) as context:
            GeneratorWrapper(
                iter([1, 2, 3]),
                consumer,
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
                consumer,
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
                    consumer,
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
                    consumer,
                ).collect(3)

            self.assertTrue(f"{GeneratorWrapper.ERROR_MULTIPLE_YIELD} (2 / 3)", str(context.exception))

    def test_working(self):
        def consumer(stream: iter):
            yield

            for x in stream:
                yield x * 2

        values, durations = GeneratorWrapper(
            iter([1, 2, 3]),
            consumer,
        ).collect(3)

        self.assertListEqual([2, 4, 6], values)
        self.assertEqual(3, len(durations))
