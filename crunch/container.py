import collections
import types
import typing

if typing.TYPE_CHECKING:
    import pandas

    from . import api

STORAGE_PROPERTY = "_storage"


def _get_storage(self: "Columns") -> typing.OrderedDict:
    return object.__getattribute__(self, STORAGE_PROPERTY)


def _to_repr(data: dict):
    items = ', '.join(f"{k}: {v!r}" for k, v in data.items())
    return "{" + str(items) + "}"


def _to_str(object: typing.Any):
    return f"{object.__class__.__name__}({object.__repr__()})"


class Columns:

    def __init__(self, storage: typing.OrderedDict, _copy=True):
        if _copy:
            storage = collections.OrderedDict(storage)

        object.__setattr__(self, STORAGE_PROPERTY, storage)

    def __getitem__(self, key):
        return _get_storage(self)[key]

    def __getattribute__(self, key):
        if key == STORAGE_PROPERTY:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

        return object.__getattribute__(self, key)

    def __getattr__(self, key):
        storage = _get_storage(self)
        if key in storage:
            return storage[key]

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{key}'")

    def __setitem__(self, key, _):
        raise AttributeError(f"Cannot set key '{key}' - object is immutable")

    def __setattr__(self, key, _):
        raise AttributeError(f"Cannot set attribute '{key}' - object is immutable")

    def __iter__(self):
        return iter(_get_storage(self).values())

    def __repr__(self):
        return _to_repr(_get_storage(self))

    def __str__(self):
        return _to_str(self)

    @staticmethod
    def from_model(column_names: "api.ColumnNames"):
        inputs = collections.OrderedDict()
        outputs = collections.OrderedDict()

        for target_column_names in column_names.targets:
            key = target_column_names.name

            inputs[key] = target_column_names.input
            outputs[key] = target_column_names.output

        return (
            Columns(inputs, _copy=False),
            Columns(outputs, _copy=False),
        )


class Features:

    def __init__(
        self,
        items: typing.List["api.DataReleaseFeature"],
        default_group_name: str
    ):
        storage = collections.defaultdict(list)  # `set` is losing order
        storage[default_group_name]

        for item in items:
            values = storage[item.group]

            if item.name not in values:
                values.append(item.name)

        self.storage = dict(storage)
        self.default_group_name = default_group_name

    def to_parameter_variants(
        self,
        base_name="feature_column_names",
        separator="_",
    ):
        parameters = {
            base_name: self.storage[self.default_group_name]
        }

        for group, names in self.storage.items():
            key = f"{base_name}{separator}{group}"

            parameters[key] = names

        return parameters

    @staticmethod
    def from_data_release(data_release: "api.DataRelease"):
        return Features(
            data_release.features,
            data_release.default_feature_group
        )


class GeneratorWrapper:

    ERROR_YIELD_MUST_BE_CALLED_BEFORE = "yield must be called once before the loop"
    ERROR_PREVIOUS_VALUE_NOT_YIELD = "previous value not yield-ed"
    ERROR_YIELD_NOT_CALLED = "yield not called"
    ERROR_FIRST_YIELD_MUST_BE_NONE = "first yield must return None"
    ERROR_MULTIPLE_YIELD = "multiple yield detected"
    ERROR_WRONG_YIELD_CALL_COUNT_PREFIX = "yield not called enough time"

    def __init__(
        self,
        iterator: typing.Iterator,
        consumer_factory: typing.Callable[[typing.Iterator], typing.Generator]
    ):
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

                yield StreamMessage(value)

        stream = inner()
        consumer = consumer_factory(stream)

        if not isinstance(consumer, types.GeneratorType):
            raise RuntimeError(self.ERROR_YIELD_NOT_CALLED)

        if next(consumer) is not None:
            raise ValueError(self.ERROR_FIRST_YIELD_MUST_BE_NONE)

        self.ready = True
        self.consumer = consumer

    def collect(
        self,
        expected_size: int
    ):
        collected = []

        for y in self.consumer:
            collected.append(y)
            self.ready = True

            if self.consumed:
                raise RuntimeError(self.ERROR_MULTIPLE_YIELD)

            self.consumed = True

        size = len(collected)
        if size != expected_size:
            raise ValueError(f"{self.ERROR_WRONG_YIELD_CALL_COUNT_PREFIX} ({size} / {expected_size})")

        return collected


class CallableIterable:

    def __init__(
        self,
        getter: typing.Callable[[], typing.Iterator],
        length: int,
    ):
        self._getter = getter
        self._length = length

    def __iter__(self):
        return self._getter()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"iterable[{self._length}]"

    def __len__(self):
        return self._length

    @staticmethod
    def from_dataframe(
        dataframe: "pandas.DataFrame",
        column_name: str
    ):
        return CallableIterable(
            lambda: dataframe[column_name].copy(),
            len(dataframe)
        )


class StreamMessage:

    x: float

    def __init__(self, x: float):
        object.__setattr__(self, "x", x)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, _):
        raise AttributeError(f"Cannot set key '{key}' - object is immutable")

    def __setattr__(self, key, _):
        raise AttributeError(f"Cannot set attribute '{key}' - object is immutable")

    def __iter__(self):
        return iter(vars(self))

    def __repr__(self):
        return _to_repr(vars(self))

    def __str__(self):
        return _to_str(self)

    def get(self, key: str, default=None):
        return getattr(self, key, default)

    def keys(self):
        return vars(self).keys()

    def values(self):
        return vars(self).values()

    def items(self):
        return vars(self).items()
