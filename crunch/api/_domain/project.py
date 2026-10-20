from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.user import User
    from crunch.api._identifiers import ProjectIdentifierType, UserIdentifierType


@dataclass
class ProjectSubmitQuotaFileLimit(JsonDataClassMixin):
    total_size: int


@dataclass
class ProjectSubmitQuotaCountLimit(JsonDataClassMixin):
    current: int
    maximum: int
    bypass: bool


@dataclass
class ProjectSubmitQuota(JsonDataClassMixin):
    code_files: ProjectSubmitQuotaFileLimit
    model_files: ProjectSubmitQuotaFileLimit
    notebook_file: ProjectSubmitQuotaFileLimit
    prediction_file: ProjectSubmitQuotaFileLimit
    submissions_per_day: ProjectSubmitQuotaCountLimit


@dataclass
class ProjectComputeQuota(JsonDataClassMixin):
    available: timedelta
    remaining: timedelta
    used: timedelta
    running: bool


class ProjectReference:
    competition = Parent["Competition"]()

    def __init__(self, competition: "Competition", user_id: int, name: str):
        self.competition = competition
        self.user_id = user_id
        self.name = name

    def reload(self) -> "Project":
        return self.competition.projects.get(self.user_id, self.name)

    @property
    def client(self) -> "Client":
        return self.competition.client

    @property
    def user(self) -> "User":
        return self.client.users.get(self.user_id)

    @property
    def submissions(self):
        from crunch.api._domain.submission import SubmissionCollection
        return SubmissionCollection(self)

    @property
    def predictions(self):
        from crunch.api._domain.prediction import PredictionCollection
        return PredictionCollection(self)

    @property
    def runs(self):
        from crunch.api._domain.run import RunCollection
        return RunCollection(self)

    @property
    def models(self):
        from crunch.api._domain.model import ModelCollection
        return ModelCollection(self)

    @property
    def selection(self) -> "ProjectSelection":
        selection = ProjectSelection.from_dict(self.client.api.get_json([
            "v4", "competitions", self.competition.id, "projects", self.user_id, self.name, "selection"
        ]))

        selection.project = self
        return selection

    @property
    def submit_quota(self) -> ProjectSubmitQuota:
        return ProjectSubmitQuota.from_dict(self.client.api.get_json([
            "v3", "competitions", self.competition.id, "projects", self.user_id, self.name, "quota", "submit"
        ]))

    @property
    def compute_quota(self) -> ProjectComputeQuota:
        return ProjectComputeQuota.from_dict(self.client.api.get_json([
            "v3", "competitions", self.competition.id, "projects", self.user_id, self.name, "quota", "compute"
        ]))

    def clone(
        self,
        *,
        submission_number: int | None,
        include_model: bool | None,
    ) -> dict[str, str]:
        return self.client.api.get_json(
            ["v4", "competitions", self.competition.id, "projects", self.user_id, self.name, "clone"],
            params={
                "submissionNumber": submission_number,
                "includeModel": include_model,
            },
        )


@dataclass
class Project(ApiModel, ProjectReference):
    id: int
    user_id: int
    name: str
    created_at: datetime

    def reload(self) -> "Project":
        return self._update_from(super().reload())


class ProjectCollection(ApiCollection[Project]):

    model = Project

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, project: Project) -> Project:
        project.competition = self.competition
        return project

    def get(
        self,
        /,
        user_identifier: "UserIdentifierType" = "@me",
        project_identifier: "ProjectIdentifierType" = "@first",
    ) -> Project:
        return self.from_dict(self.client.api.get_json(["v3", "competitions", self.competition.id, "projects", user_identifier, project_identifier]))

    def list(
        self,
        /,
        user_identifier: "UserIdentifierType" = "@me",
    ) -> list[Project]:
        return self.from_dict_array(self.client.api.get_json(["v3", "competitions", self.competition.id, "projects", user_identifier]))

    def reference(
        self,
        /,
        user_id: int,
        name: str,
    ) -> ProjectReference:
        return ProjectReference(self.competition, user_id, name)


class ProjectSelectionOrigin(ApiEnum):

    SELECTION = "SELECTION"
    """Explicitly selected for the current round."""

    PREVIOUS_ROUND = "PREVIOUS_ROUND"
    """Carried over from the selection of the previous round."""

    LAST_RUN = "LAST_RUN"
    """Nothing selected, the last successful run is used by default."""


@dataclass
class ProjectSelectionSubmission(JsonDataClassMixin):
    id: int
    number: int


@dataclass
class ProjectSelection(ApiModel):
    origin: ProjectSelectionOrigin
    runnable: bool
    run_id: int | None = None
    submission: ProjectSelectionSubmission | None = None
    train_frequency: int | None = None
    force_first_train: bool | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    project = Parent["ProjectReference"]()

    @property
    def submission_number(self) -> int | None:
        return self.submission.number if self.submission is not None else None

    @property
    def run(self):
        if self.run_id is None:
            return None

        return self.project.runs.get(self.run_id)


class ProjectTokenType(ApiEnum):
    TEMPORARY = "TEMPORARY"
    PERMANENT = "PERMANENT"


@dataclass
class ProjectToken(ApiModel):
    id: int
    type: ProjectTokenType
    plain: str | None = field(default=None, repr=False)
    valid_until: datetime | None = None

    project = Parent["Project"]()


class ProjectTokenCollection(ApiCollection[ProjectToken]):

    model = ProjectToken

    def upgrade(
        self,
        clone_token: str,
    ) -> ProjectToken:
        payload = self.client.api.post_json(
            ["v2", "project-tokens", "upgrade"],
            json={"cloneToken": clone_token},
        )

        project_token = self.from_dict(payload)

        # the embedded project only knows its competition by id
        project_payload = payload.get("project")
        if project_payload is not None:
            competition = self.client.competitions.get(project_payload["competitionId"])
            project_token.project = competition.projects.from_dict(project_payload)

        return project_token
