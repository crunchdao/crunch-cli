import abc
import typing

import pandas

from .. import api
from .collector import PredictionCollector


class Runner(abc.ABC):

    force_first_train: bool
    column_names: api.ColumnNames

    def __init__(
        self,
        prediction_collector: PredictionCollector,
        competition_format: api.CompetitionFormat,
        determinism_check_enabled=False,
    ):
        self.prediction_collector = prediction_collector
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

        try:
            if self.competition_format == api.CompetitionFormat.TIMESERIES:
                self.log("starting timeseries loop...")
                self.start_timeseries()

            elif self.competition_format == api.CompetitionFormat.DAG:
                self.log("starting dag process...")
                self.start_dag()

            elif self.competition_format == api.CompetitionFormat.STREAM:
                self.log("starting stream loop...")
                self.start_stream()

            elif self.competition_format == api.CompetitionFormat.SPATIAL:
                self.log("starting spatial loop...")
                self.start_spatial()

            else:
                raise ValueError(f"unsupported: {self.competition_format}")
        except:
            self.prediction_collector.discard()
            raise

        if self.determinism_check_enabled:
            if self.deterministic:
                self.log(f"determinism check: passed")
            else:
                self.log(f"determinism check: failed", error=True)

        self.finalize()
        self.log("ended")

        self.teardown()

    def start_timeseries(self):
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

            self.prediction_collector.append(prediction)

    @abc.abstractmethod
    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        ...

    def start_dag(self):
        prediction = self.dag_loop(True)
        self.prediction_collector.append(prediction)

        if self.deterministic:
            prediction2 = self.dag_loop(False)
            self.deterministic = prediction.equals(prediction2)
            self.log(f"deterministic: {str(self.deterministic).lower()}")

    @abc.abstractmethod
    def dag_loop(
        self,
        train: bool
    ) -> pandas.DataFrame:
        ...

    def start_stream(self):
        if not self.have_model:
            self.stream_no_model()

        target_column_namess = self.column_names.targets
        for index, target_column_names in enumerate(target_column_namess):
            self.log(f"looping stream=`{target_column_names.name}` ({index + 1}/{len(target_column_namess)})")

            prediction = self.stream_loop(target_column_names)

            if self.deterministic:
                prediction2 = self.stream_loop(target_column_names)
                self.deterministic = prediction.equals(prediction2)
                self.log(f"deterministic: {str(self.deterministic).lower()}")

            self.prediction_collector.append(prediction)

    def stream_have_model(self):
        return self.have_model

    @abc.abstractmethod
    def stream_no_model(
        self,
    ):
        ...

    @abc.abstractmethod
    def stream_loop(
        self,
        target_column_name: api.TargetColumnNames,
    ) -> pandas.DataFrame:
        ...

    def start_spatial(self):
        if self.force_first_train:
            self.spatial_train()

        target_column_namess = self.column_names.targets
        require_target_column = len(target_column_namess) > 1

        for index, target_column_names in enumerate(target_column_namess):
            self.log(f"looping target=`{target_column_names.name}` ({index + 1}/{len(target_column_namess)})")

            prediction = self.spatial_loop(target_column_names)

            if self.deterministic:
                prediction2 = self.spatial_loop(target_column_names)
                self.deterministic = prediction.equals(prediction2)
                self.log(f"deterministic: {str(self.deterministic).lower()}")

            if require_target_column:
                prediction["sample"] = target_column_names.name

            self.prediction_collector.append(prediction)

    @abc.abstractmethod
    def spatial_train(
        self,
    ) -> None:
        ...

    @abc.abstractmethod
    def spatial_loop(
        self,
        target_column_names: api.TargetColumnNames
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
    def finalize(self):
        ...

    def teardown(self):
        ...

    @abc.abstractmethod
    def log(self, message: str, important=False, error=False):
        ...
