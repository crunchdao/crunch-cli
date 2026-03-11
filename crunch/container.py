from datetime import datetime, timedelta
from types import GeneratorType
from typing import Any, Callable, Generator, Iterator, List, Optional


class GeneratorWrapper:

    ERROR_YIELD_MUST_BE_CALLED_BEFORE = "yield must be called once before the loop"
    ERROR_PREVIOUS_VALUE_NOT_YIELD = "previous value not yield-ed"
    ERROR_YIELD_NOT_CALLED = "yield not called"
    ERROR_FIRST_YIELD_MUST_BE_NONE = "first yield must return None"
    ERROR_MULTIPLE_YIELD = "multiple yield detected"
    ERROR_WRONG_YIELD_CALL_COUNT_PREFIX = "yield not called enough time"

    def __init__(
        self,
        iterator: Iterator,
        consumer_factory: Callable[[Iterator], Generator],
        *,
        element_wrapper_factory: Optional[Callable[[Any], Any]] = None,
        post_processor: Optional[Callable[[Any], Any]] = None,
    ):
        self.element_wrapper_factory = element_wrapper_factory or (lambda x: x)
        self.post_processor = post_processor or (lambda x: x)

        self.ready = None
        self.consumed = True

        def inner():
            for value in iterator:
                if self.ready is None:
                    raise RuntimeError(self.ERROR_YIELD_MUST_BE_CALLED_BEFORE)

                if not self.ready:
                    raise RuntimeError(self.ERROR_PREVIOUS_VALUE_NOT_YIELD)

                self.ready = False
                self.consumed = False

                yield self.element_wrapper_factory(value)

        stream = inner()
        consumer = consumer_factory(stream)

        if not isinstance(consumer, GeneratorType):
            raise RuntimeError(self.ERROR_YIELD_NOT_CALLED)

        if next(consumer) is not None:
            raise ValueError(self.ERROR_FIRST_YIELD_MUST_BE_NONE)

        self.ready = True
        self.consumer = consumer

    def collect(
        self,
        expected_size: int
    ):
        values: List[Any] = []
        durations: List[timedelta] = []

        sentinel = object()

        iterator = self.consumer
        while True:
            start = datetime.now()

            y = next(iterator, sentinel)
            if y is sentinel:
                break

            took = datetime.now() - start

            y = self.post_processor(y)

            values.append(y)
            durations.append(took)

            self.ready = True

            if self.consumed:
                raise RuntimeError(self.ERROR_MULTIPLE_YIELD)

            self.consumed = True

        size = len(values)
        if size != expected_size:
            raise ValueError(f"{self.ERROR_WRONG_YIELD_CALL_COUNT_PREFIX} ({size} / {expected_size})")

        return values, durations
