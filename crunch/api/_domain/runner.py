from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from crunch.api._domain.data_release import DataRelease
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._client import Client


class RunnerRunSpanStatus(Enum):
    STARTED = "STARTED"
    ENDED = "ENDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class RunnerRunSpan(JsonDataClassMixin):
    id: int
    parent_id: int | None
    description: str
    started_at: datetime
    attributes: dict[str, Any] | None


@dataclass(frozen=True)
class StartedRunnerRunSpan(RunnerRunSpan):
    status: Literal[RunnerRunSpanStatus.STARTED] = RunnerRunSpanStatus.STARTED


@dataclass(frozen=True)
class EndedRunnerRunSpan(RunnerRunSpan):
    status: Literal[RunnerRunSpanStatus.ENDED, RunnerRunSpanStatus.FAILED]
    ended_at: datetime
    error: str | None


@dataclass(frozen=True)
class RunnerRunMetric(JsonDataClassMixin):
    timestamp: datetime
    cpu: float  # % 0–100
    ram: int  # bytes used
    disk: int  # bytes used
    gpu: float | None  # % 0–100
    vram: int | None  # bytes used


class RunnerRun:

    def __init__(
        self,
        id: int,
        client: "Client",
    ):
        self.id = id
        self.client = client

    @property
    def code(self) -> dict[str, str]:
        return self.client.api.get_json([
            "v1", "runner", "runs", self.id, "code"
        ])

    @property
    def model(self) -> dict[str, str]:
        return self.client.api.get_json([
            "v1", "runner", "runs", self.id, "model"
        ])

    @property
    def data(self) -> DataRelease:
        return DataRelease.from_dict(self.client.api.get_json([
            "v1", "runner", "runs", self.id, "data"
        ]))

    def report_error(
        self,
        *,
        trace: str,
    ) -> None:
        self.client.api.call(
            "PUT",
            ["v1", "runner", "runs", self.id, "error"],
            json={
                "trace": trace
            }
        )

    def report_traces(
        self,
        *,
        spans: list[RunnerRunSpan],
        metrics: list[RunnerRunMetric],
    ) -> None:
        self.client.api.call(
            "POST",
            ["v1", "runner", "runs", self.id, "traces"],
            json={
                "spans": [
                    span.to_dict()
                    for span in spans
                ],
                "metrics": [
                    metric.to_dict()
                    for metric in metrics
                ],
            },
        )

    def submit_result(
        self,
        *,
        use_initial_model: bool,
        deterministic: bool | None,
        prediction_files: dict[str, str],
        model_files: dict[str, str],
    ) -> None:
        self.client.api.call(
            "POST",
            ["v1", "runner", "runs", self.id, "result"],
            json={
                "useInitialModel": use_initial_model,
                "deterministic": deterministic,
                "predictionFiles": prediction_files,
                "modelFiles": model_files,
            },
        )
