from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, TypedDict

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._domain.prediction import Prediction
from crunch.api._domain.runtime import RuntimeType
from crunch.api._domain.submission import Submission
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._domain.project import ProjectReference, ProjectSelection


class RunStatus(ApiEnum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CLEANING = "CLEANING"
    COMPLETED = "COMPLETED"


class RunLog(TypedDict):
    id: int
    emitter: str
    error: bool
    content: str
    createdAt: str  # TODO Move to camel_case


@dataclass
class RunLogs:
    entries: list[RunLog]
    cursor: str | None


class RunTraceSpanStatus(ApiEnum):
    STARTED = "STARTED"
    ENDED = "ENDED"
    FAILED = "FAILED"


@dataclass
class RunTraceSpan(JsonDataClassMixin):
    id: int
    parent_id: int | None
    description: str
    status: RunTraceSpanStatus
    started_at: datetime
    ended_at: datetime | None = None
    attributes: dict[str, Any] | None = None
    error: str | None = None


@dataclass
class RunTraceMetrics(JsonDataClassMixin):
    """Resource usage samples, one column per resource: `cpu` and `gpu` in percent, `ram`, `disk` and `vram` in bytes."""

    timestamp: list[datetime]
    cpu: list[float | None]
    ram: list[int | None]
    disk: list[int | None]
    gpu: list[float | None] | None = None
    vram: list[int | None] | None = None


@dataclass
class RunTraces(JsonDataClassMixin):
    spans: list[RunTraceSpan]
    metrics: RunTraceMetrics


@dataclass
class Run(ApiModel):
    id: int
    status: RunStatus
    success: bool
    terminated: bool
    submission: Submission
    created_at: datetime
    prediction: Prediction | None = None
    error_message: str | None = None
    error_trace: str | None = None
    exit_code: int | None = None
    exit_reason: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration: timedelta | None = None
    runtime_type: RuntimeType | None = None
    cpu: int | None = None
    """In 1024ths of a core, known once the run started."""
    memory: int | None = None
    """In MiB, known once the run started."""

    project = Parent["ProjectReference"]()

    def reload(self) -> "Run":
        return self._update_from(self.project.runs.get(self.id))

    @property
    def logs(self) -> list[RunLog]:
        return self.project.client.api.get_json(["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs", self.id, "logs"])

    def get_logs(
        self,
        *,
        cursor: str | None = None,
    ) -> RunLogs:
        response = self.project.client.api.get_json(
            ["v4", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs", self.id, "logs"],
            params={
                "cursor": cursor
            },
        )

        archive_url = response.get("archiveUrl")
        if archive_url is not None:
            import requests

            archive_response = requests.get(archive_url)
            archive_response.raise_for_status()

            return RunLogs(
                entries=archive_response.json(),
                cursor=None,
            )

        return RunLogs(
            entries=response["entries"],
            cursor=response["cursor"],
        )

    def get_traces(self) -> RunTraces:
        return RunTraces.from_dict(self.project.client.api.get_json(
            ["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs", self.id, "traces"],
        ))

    def select(self) -> "ProjectSelection":
        from crunch.api._domain.project import ProjectSelection

        selection = ProjectSelection.from_dict(self.project.client.api.post_json(
            ["v4", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "selection"],
            json={
                "runId": self.id
            },
        ))

        selection.project = self.project
        return selection

    def terminate(self) -> "Run":
        return self._update_from(self.project.runs.from_dict(self.project.client.api.delete_json(
            ["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs", self.id],
        )))


class RunCollection(ApiCollection[Run]):

    model = Run

    def __init__(self, project: "ProjectReference"):
        super().__init__(project.client)

        self.project = project

    def _attach(self, run: Run) -> Run:
        run.project = self.project

        run.submission.project = self.project
        if run.submission.model is not None:
            run.submission.model.project = self.project

        if run.prediction is not None:
            run.prediction.project = self.project

        return run

    def get(
        self,
        id: int,
    ) -> Run:
        return self.from_dict(self.client.api.get_json(["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs", id]))

    def create(
        self,
        *,
        submission: Submission,
        train_frequency: int | None = None,
        force_first_train: bool | None = None,
        runtime_definition_name: str | None = None,
    ) -> Run:
        return self.from_dict(self.client.api.post_json(
            ["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs"],
            json={
                "submissionId": submission.id,
                "trainFrequency": train_frequency,
                "forceFirstTrain": force_first_train,
                "runtimeDefinitionName": runtime_definition_name,
            },
        ))

    def list(
        self,
        *,
        managed: bool | None = None,
        submission: "Submission | int | None" = None,
    ) -> list[Run]:
        submission_number = submission if isinstance(submission, int) or submission is None else submission.number

        return self.from_dict_array(self.client.api.get_json(
            ["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs"],
            params={
                "managed": managed,
                "submissionNumber": submission_number,
            },
        ))
