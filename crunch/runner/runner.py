import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any

from crunch.api import CompetitionFormat

if TYPE_CHECKING:
    from crunch.runner.tracing import RunnerTracer


class Runner(ABC):

    started_at: datetime

    def __init__(
        self,
        *,
        competition_format: CompetitionFormat,
        tracer: "RunnerTracer",
        logger: logging.Logger,
        determinism_check_enabled: bool = False,
    ):
        self.competition_format = competition_format
        self.tracer = tracer
        self.logger = logger

        self.determinism_check_enabled = determinism_check_enabled
        self.deterministic = True if determinism_check_enabled else None

    def start(self):
        self.started_at = datetime.now()

        with self.tracer:
            self.setup()
            self.logger.info("started")

            self.initialize()

            if self.competition_format != CompetitionFormat.UNSTRUCTURED:
                raise NotImplementedError(f"{self.competition_format.name} format is not supported anymore.")

            self.start_unstructured()

            if self.determinism_check_enabled:
                if self.deterministic:
                    self.logger.info("determinism check: passed")
                else:
                    self.logger.error("determinism check: failed")

            self.finalize()

            self.logger.info("ended")
            self.teardown()

    @abstractmethod
    def start_unstructured(self) -> None:
        ...

    def setup(self):
        ...

    @abstractmethod
    def initialize(self):
        ...

    @abstractmethod
    def finalize(self):
        ...

    def teardown(self):
        ...

    def _span(self, description: str, attributes: dict[str, Any] | None = None, skip: bool = False):
        return self.tracer.span(description, attributes=attributes, skip=skip)
