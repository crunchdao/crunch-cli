import abc
import typing

import pandas

from .. import api


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
