from abc import ABC, abstractmethod
from typing import List, Literal, Tuple, Union

from crunch.api import CompetitionFormat


class Runner(ABC):

    def __init__(
        self,
        *,
        competition_format: CompetitionFormat,
        prediction_directory_path: str,
        determinism_check_enabled: bool = False,
    ):
        self.competition_format = competition_format

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

        if self.competition_format != CompetitionFormat.UNSTRUCTURED:
            raise NotImplementedError(f"{self.competition_format.name} format is not supported anymore.")

        self.start_unstructured()

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
