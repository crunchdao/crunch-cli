from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from crunch.api._resource import Collection, EndpointMixin, Model
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.user import User
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


@dataclass
class FileLimit(JsonDataClassMixin):
    total_size: int


@dataclass
class CountLimit(JsonDataClassMixin):
    current: int
    maximum: int
    bypass: bool


@dataclass
class ProjectSubmitQuota(JsonDataClassMixin):
    code_files: FileLimit
    model_files: FileLimit
    notebook_file: FileLimit
    prediction_file: FileLimit
    submissions_per_day: CountLimit


@dataclass
class ProjectComputeQuota(JsonDataClassMixin):
    available: int
    remaining: int
    used: int
    running: bool


class Project(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "ProjectCollection | None" = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def resource_identifier(self) -> tuple[int, str]:
        return (self.user_id, self.name)

    @property
    def competition(self):
        return self._competition

    @property
    def user_id(self) -> int:
        return self._attrs["userId"]

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def user(self) -> "User":
        return self._checked_client.users.get(self.user_id)

    @property
    def created_at(self) -> datetime:
        return datetime.fromisoformat(self._attrs["createdAt"])

    @property
    def submissions(self):
        from crunch.api._domain.submission import SubmissionCollection

        return SubmissionCollection(
            project=self,
            client=self._client
        )

    @property
    def predictions(self):
        from crunch.api._domain.prediction import PredictionCollection

        return PredictionCollection(
            project=self,
            client=self._client
        )

    @property
    def runs(self):
        from crunch.api._domain.run import RunCollection

        return RunCollection(
            project=self,
            client=self._client
        )

    @property
    def submit_quota(self) -> ProjectSubmitQuota:
        return ProjectSubmitQuota.from_dict(
            cast("Attrs", self._checked_client.api.get_project_submit_quota(
                self.competition.id,
                self.user_id,
                self.name,
            ))
        )

    @property
    def compute_quota(self) -> ProjectComputeQuota:
        return ProjectComputeQuota.from_dict(
            cast("Attrs", self._checked_client.api.get_project_compute_quota(
                self.competition.id,
                self.user_id,
                self.name,
            ))
        )

    @property
    def selection(self) -> "ProjectSelection":
        return ProjectSelection(
            self,
            cast("Attrs", self._checked_client.api.get_project_selection(
                self.competition.id,
                self.user_id,
                self.name,
            )),
            client=self._client,
        )

    def clone(
        self,
        submission_number: int | None,
        include_model: bool | None,
    ) -> dict[str, str]:
        return self._checked_client.api.clone_project(
            self.competition.id,
            self.user_id,
            self.name,
            submission_number,
            include_model,
        )  # pyright: ignore[reportReturnType]


class ProjectSelectionOrigin(Enum):

    SELECTION = "SELECTION"
    """Explicitly selected for the current round."""

    PREVIOUS_ROUND = "PREVIOUS_ROUND"
    """Carried over from the selection of the previous round."""

    LAST_RUN = "LAST_RUN"
    """Nothing selected, the last successful run is used by default."""

    def __repr__(self):
        return self.name


class ProjectSelection(Model[None]):

    def __init__(
        self,
        project: Project,
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
    ):
        super().__init__(attrs=attrs, client=client)

        self._project = project

    def __repr__(self):
        return f"ProjectSelection(run_id={self.run_id}, origin={self.origin!r})"

    @property
    def project(self):
        return self._project

    @property
    def origin(self):
        return ProjectSelectionOrigin[self._attrs["origin"]]

    @property
    def run_id(self) -> int | None:
        return self._attrs.get("runId")

    @property
    def run(self):
        run_id = self.run_id
        if run_id is None:
            return None

        return self._project.runs.get(run_id)

    @property
    def submission_number(self) -> int | None:
        submission = self._attrs.get("submission")
        return submission["number"] if submission else None

    @property
    def train_frequency(self) -> int | None:
        return self._attrs.get("trainFrequency")

    @property
    def force_first_train(self) -> bool | None:
        return self._attrs.get("forceFirstTrain")

    @property
    def runnable(self) -> bool:
        return bool(self._attrs.get("runnable"))

    @property
    def created_at(self) -> datetime | None:
        created_at = self._attrs.get("createdAt")
        return datetime.fromisoformat(created_at) if created_at else None

    @property
    def updated_at(self) -> datetime | None:
        updated_at = self._attrs.get("updatedAt")
        return datetime.fromisoformat(updated_at) if updated_at else None


class ProjectCollection(Collection[Project]):

    model = Project

    def __init__(
        self,
        competition: "Competition",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.competition = competition

    def get(
        self,
        user_identifier: "UserIdentifierType" = "@me",
        project_identifier: "ProjectIdentifierType" = "@first"
    ) -> Project:
        return self.prepare_model(
            self._checked_client.api.get_project(
                self.competition.id,
                user_identifier,
                project_identifier
            )
        )

    def list(
        self,
        user_identifier: int | str = "@me",
    ) -> list[Project]:
        return self.prepare_models(
            self._checked_client.api.list_projects(
                self.competition.id,
                user_identifier
            )
        )

    def prepare_model(self, attrs: "JsonValue | Project", *args: Any):
        return super().prepare_model(
            attrs,
            self.competition,
            *args
        )


class ProjectTokenType(Enum):

    TEMPORARY = "TEMPORARY"
    PERMANENT = "PERMANENT"

    def __repr__(self):
        return self.name


class ProjectToken(Model[int]):

    def __init__(
        self,
        competition: "Competition | None",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "ProjectTokenCollection | None" = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def project(self) -> Project:
        project_attrs = self._attrs["project"]

        competition_id = project_attrs["competitionId"]
        competition = self._checked_client.competitions.get(competition_id)

        return competition.projects.prepare_model(
            project_attrs
        )

    @property
    def plain(self) -> str | None:
        return self._attrs.get("plain")

    @property
    def type(self):
        return ProjectTokenType[self._attrs["type"]]

    @property
    def valid_until(self):
        value = self._attrs.get("validUntil")
        if value is None:
            return None

        return datetime.fromisoformat(value)


class ProjectTokenCollection(Collection[ProjectToken]):

    model = ProjectToken

    def __init__(
        self,
        competition: "Competition | None",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.competition = competition

    def upgrade(
        self,
        clone_token: str
    ) -> ProjectToken:
        return self.prepare_model(
            self._checked_client.api.upgrade_project_token(
                clone_token
            ),
            self.competition
        )


class ProjectEndpointMixin(EndpointMixin):

    def get_project(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}"
            ),
            json=True,
        )

    def list_projects(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}"
            ),
            json=True,
        )

    def upgrade_project_token(
        self,
        clone_token: str,
    ):
        return self._result(
            self.post(
                f"/v2/project-tokens/upgrade",
                json={
                    "cloneToken": clone_token
                }
            ),
            json=True
        )

    def clone_project(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        submission_number: int | None,
        include_model: bool | None,
    ):
        params: dict[str, Any] = {}

        if submission_number is not None:
            params["submissionNumber"] = submission_number

        if include_model is not None:
            params["includeModel"] = include_model

        return self._result(
            self.get(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/clone",
                params=params
            ),
            json=True,
        )

    def get_project_submit_quota(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/quota/submit",
            ),
            json=True,
        )

    def get_project_compute_quota(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/quota/compute",
            ),
            json=True,
        )

    def get_project_selection(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/selection",
            ),
            json=True,
        )
