import os
from abc import ABC, abstractmethod
from typing import List, Literal, Tuple, Union

import pandas

from crunch.api import ColumnNames, CompetitionFormat
from crunch.runner.collector import MemoryPredictionCollector


class Runner(ABC):

    def __init__(
        self,
        *,
        competition_format: CompetitionFormat,
        prediction_directory_path: str,
        determinism_check_enabled: bool = False,
    ):
        self.competition_format = competition_format

        # TODO Remove this when TIMESERIES competition are removed
        prediction_parquet_file_path = os.path.join(prediction_directory_path, "prediction.parquet")
        self.prediction_parquet_file_path = prediction_parquet_file_path
        self.prediction_directory_path = prediction_directory_path

        self.determinism_check_enabled = determinism_check_enabled
        self.deterministic = True if determinism_check_enabled else None

    def start(self):
        self.setup()
        self.log("started")

        (
            self.keys,
            self.has_model,
        ) = self.initialize()

        # TODO Remove this when TIMESERIES competition are removed
        if self.competition_format == CompetitionFormat.TIMESERIES:
            self.log("starting timeseries loop...")
            self.start_timeseries()

        elif self.competition_format == CompetitionFormat.UNSTRUCTURED:
            self.log("starting unstructured loop...")
            self.start_unstructured()

        else:
            raise NotImplementedError(f"{self.competition_format.name} format is not supported anymore.")

        if self.determinism_check_enabled:
            if self.deterministic:
                self.log(f"determinism check: passed")
            else:
                self.log(f"determinism check: failed", error=True)

        self.finalize()
        self.log("ended")

        self.teardown()

    force_first_train: bool
    train_frequency: int
    column_names: ColumnNames

    def start_timeseries(self) -> None:
        collector = MemoryPredictionCollector()

        try:
            for index, moon in enumerate(self.keys):
                train, forced_train = False, False
                if self.train_frequency != 0 and moon % self.train_frequency == 0:
                    train = True
                elif index == 0 and not self.has_model:
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

                collector.append(prediction)
        except:
            collector.discard()
            raise

        self.log(f"save prediction - path={self.prediction_parquet_file_path}", important=True)
        collector.persist(self.prediction_parquet_file_path)

    @abstractmethod
    def timeseries_loop(
        self,
        moon: int,
        train: bool
    ) -> pandas.DataFrame:
        ...

    @abstractmethod
    def start_unstructured(self) -> None:
        ...

    def setup(self):
        ...

    @abstractmethod
    def initialize(self) -> Tuple[
        List[Union[str, int]],  # keys
        bool,  # has_model
    ]:
        ...

    @abstractmethod
    def finalize(self):
        ...

    def teardown(self):
        ...

    @abstractmethod
    def log(
        self,
        message: str,
        *,
        important: bool = False,
        error: bool = False,
    ) -> Literal[True]:
        ...
