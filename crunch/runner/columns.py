import collections
import typing


STORAGE_PROPERTY = "_storage"


def _get_storage(self: "Columns") -> typing.OrderedDict:
    return object.__getattribute__(self, STORAGE_PROPERTY)


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
        items = ', '.join(f"{k}: {v!r}" for k, v in _get_storage(self).items())
        return "{" + str(items) + "}"

    def __str__(self):
        return f"{self.__class__.__name__}({self.__repr__()})"

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
