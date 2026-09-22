from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict, Union

from crunch.api._resource import Collection, EndpointMixin
from crunch.api._resource import Model as BaseModel

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.prediction import Prediction
    from crunch.api._domain.project import Project
    from crunch.api._domain.submission import Submission
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class RunStatus(Enum):

    CREATED = "CREATED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    CLEANING = "CLEANING"
    COMPLETED = "COMPLETED"

    def __repr__(self):
        return self.name


class RunLog(TypedDict):
    id: int
    emitter: str
    error: bool
    content: str
    createdAt: str  # TODO Move to camel_case


class Run(BaseModel[int]):

    def __init__(
        self,
        project: "Project",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["RunCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._project = project

    @property
    def project(self):
        return self._project

    @property
    def success(self) -> bool:
        return self._attrs["success"]

    @property
    def terminated(self) -> bool:
        return self._attrs["terminated"]

    @property
    def error_message(self) -> Optional[str]:
        return self._attrs["errorMessage"]

    @property
    def error_trace(self) -> Optional[str]:
        return self._attrs["errorTrace"]

    @property
    def status(self) -> Optional[RunStatus]:
        return RunStatus(self._attrs["status"])

    @property
    def duration(self) -> Optional[timedelta]:
        value = self._attrs["duration"]
        if value is None:
            return None

        return timedelta(seconds=value)

    @property
    def submission(self) -> "Submission":
        from crunch.api._domain.submission import Submission

        return Submission(self._project, self._attrs["submission"], self._client)

    @property
    def prediction(self) -> Optional["Prediction"]:
        from crunch.api._domain.prediction import Prediction

        prediction_attrs = self._attrs["prediction"]
        if prediction_attrs is None:
            return None

        return Prediction(self._project, prediction_attrs, self._client)

    @property
    def started_at(self) -> Optional[datetime]:
        value = self._attrs["startedAt"]
        if value is None:
            return None

        return datetime.fromisoformat(value)

    @property
    def ended_at(self) -> Optional[datetime]:
        value = self._attrs["endedAt"]
        if value is None:
            return None

        return datetime.fromisoformat(value)

    @property
    def exit_code(self) -> Optional[int]:
        return self._attrs["exitCode"]

    @property
    def exit_reason(self) -> Optional[str]:
        return self._attrs["exitReason"]

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self._attrs["createdAt"])

    @property
    def logs(self) -> List["RunLog"]:
        return self._checked_client.api.get_run_logs(
            self._project.competition.id,
            self._project.user_id,
            self._project.name,
            self.id
        )

    def select(self):
        self._attrs.update(
            self._checked_client.api.select_run(
                self._project.competition.id,
                self._project.user_id,
                self._project.name,
                self.id
            )
        )

    def terminate(self):
        self._attrs.update(
            self._checked_client.api.terminate_run(
                self._project.competition.id,
                self._project.user_id,
                self._project.name,
                self.id
            )
        )


class RunCollection(Collection[Run]):

    model = Run

    def __init__(
        self,
        project: "Project",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.project = project

    def get(
        self,
        id: int
    ) -> Run:
        return self.prepare_model(
            self._checked_client.api.get_run(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                id
            )
        )

    def create(
        self,
        *,
        submission: "Submission",
        train_frequency: Optional[int] = None,
        force_first_train: Optional[bool] = None,
        runtime_definition_name: Optional[str] = None,
    ) -> Run:
        return self.prepare_model(
            self._checked_client.api.create_run(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                submission.id,
                train_frequency,
                force_first_train,
                runtime_definition_name,
            )
        )

    def list(
        self,
        managed: Optional[bool] = None,
        submission: Optional["Submission"] = None,
        submission_number: Optional[int] = None
    ) -> List[Run]:
        assert not ((submission is not None) and (submission_number is not None))

        return self.prepare_models(
            self._checked_client.api.list_runs(
                self.project.competition.id,
                self.project.user_id,
                self.project.name,
                managed,
                submission.number if submission is not None else submission_number,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Run], *args: Any) -> Run:
        return super().prepare_model(
            attrs,
            self.project,
            *args
        )


class RunEndpointMixin(EndpointMixin):

    def create_run(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        submission_id: int,
        train_frequency: Optional[int] = None,
        force_first_train: Optional[bool] = None,
        runtime_definition_name: Optional[str] = None,
    ):
        return self._result(
            self.post(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs",
                json={
                    "submissionId": submission_id,
                    "trainFrequency": train_frequency,
                    "forceFirstTrain": force_first_train,
                    "runtimeDefinitionName": runtime_definition_name,
                }
            ),
            json=True,
        )

    def list_runs(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        managed: Optional[bool],
        submission_number: Optional[int]
    ):
        params: Dict[str, Any] = {}

        if managed is not None:
            params["managed"] = managed

        if submission_number is not None:
            params["submissionNumber"] = submission_number

        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs",
                params=params
            ),
            json=True
        )

    def get_run(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        run_id: int
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs/{run_id}"
            ),
            json=True
        )

    def get_run_logs(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        run_id: int
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs/{run_id}/logs"
            ),
            json=True
        )

    def select_run(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        run_id: int
    ):
        return self._result(
            self.post(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/selection",
                json={"runId": run_id}
            ),
            json=True
        )

    def terminate_run(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        run_id: int
    ):
        return self._result(
            self.delete(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/runs/{run_id}"
            ),
            json=True
        )
