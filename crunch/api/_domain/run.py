from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypedDict

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._domain.prediction import Prediction
from crunch.api._domain.submission import Submission

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

    project = Parent["ProjectReference"]()

    def reload(self) -> "Run":
        return self._update_from(self.project.runs.get(self.id))

    @property
    def logs(self) -> list[RunLog]:
        return self.project.client.api.get_json(["v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "runs", self.id, "logs"])

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
