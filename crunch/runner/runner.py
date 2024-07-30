import abc
import typing

import pandas

from .. import api
from ..container import Columns


class Runner(abc.ABC):

    def __init__(
        self,
        competition_format: api.CompetitionFormat,
        determinism_check_enabled=False,
    ):
        self.competition_format = competition_format

        self.determinism_check_enabled = determinism_check_enabled
        self.deterministic = True if determinism_check_enabled else None

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
        
        if self.determinism_check_enabled:
            if self.deterministic:
                self.log(f"determinism check: passed")
            else:
                self.log(f"determinism check: failed", error=True)

        prediction = self.finalize(prediction)
        self.log("ended")

        self.teardown()

        return prediction

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

            prediction = self.timeseries_loop(moon, train)

            if self.deterministic:
                prediction2 = self.timeseries_loop(moon, False)
                self.deterministic = prediction.equals(prediction2)
                self.log(f"deterministic: {str(self.deterministic).lower()}")

            predictions.append(prediction)

        return pandas.concat(predictions)

    @abc.abstractmethod
    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        ...

    def start_dag(self):
        prediction = self.dag_loop(True)

        if self.deterministic:
            prediction2 = self.dag_loop(False)
            self.deterministic = prediction.equals(prediction2)
            self.log(f"deterministic: {str(self.deterministic).lower()}")

        return prediction

    @abc.abstractmethod
    def dag_loop(
        self,
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
