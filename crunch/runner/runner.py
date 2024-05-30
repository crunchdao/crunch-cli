import abc
import collections
import typing

import pandas

from .. import api

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

        for key, value in column_names.targets.items():
            inputs[key] = value.input
            outputs[key] = value.output

        return (
            Columns(inputs, _copy=False),
            Columns(outputs, _copy=False),
        )


class Runner(abc.ABC):

    def __init__(
        self,
        competition_format: api.CompetitionFormat
    ):
        self.competition_format = competition_format

    def start(self):
        self.setup()
        self.log("started")

        (
            self.keys,
            self.have_model,
        ) = self.initialize()

        if self.competition_format == api.CompetitionFormat.TIMESERIES:
            self.log("starting timeseries loop...")
            prediction = self.start_timeseries()

        elif self.competition_format == api.CompetitionFormat.DAG:
            self.log("starting dag process...")
            prediction = self.start_dag()

        else:
            raise ValueError(f"unsupported: {self.competition_format}")

        prediction = self.finalize(prediction)
        self.log("ended")

        self.teardown()

    def start_timeseries(self):
        predictions: typing.List[pandas.DataFrame] = []

        for index, moon in enumerate(self.keys):
            train, forced_train = False, False
            if self.train_frequency != 0 and moon % self.train_frequency == 0:
                train = True
            elif index == 0 and not self.have_model:
                train = True
            elif index == 0 and self.force_first_train:
                train, forced_train = True, True

            forced_train = " forced=True" if forced_train else ""
            self.log(f"looping moon={moon} train={train}{forced_train} ({index + 1}/{len(self.keys)})")

            prediction = self.timeseries_loop(
                moon,
                train,
            )

            predictions.append(prediction)

        return pandas.concat(predictions)

    @abc.abstractmethod
    def start_dag(self):
        ...

    @abc.abstractmethod
    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        ...

    def setup(self):
        ...

    @abc.abstractmethod
    def initialize(self) -> typing.Tuple[
        typing.List[typing.Union[str, int]],  # keys
        bool,  # have_model
    ]:
        ...

    @abc.abstractmethod
    def finalize(self, prediction: pandas.DataFrame):
        ...

    def teardown(self):
        ...

    @abc.abstractmethod
    def log(self, message: str, error=False):
        ...
