from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from dataclasses_json import LetterCase, Undefined, dataclass_json

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.user import User
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


@dataclass_json(undefined=Undefined.EXCLUDE, letter_case=LetterCase.CAMEL)  # type: ignore[call-overload]
@dataclass
class FileLimit:
    total_size: int


@dataclass_json(undefined=Undefined.EXCLUDE, letter_case=LetterCase.CAMEL)  # type: ignore[call-overload]
@dataclass
class CountLimit:
    current: int
    maximum: int
    bypass: bool


@dataclass_json(undefined=Undefined.EXCLUDE, letter_case=LetterCase.CAMEL)  # type: ignore[call-overload]
@dataclass
class ProjectSubmitQuota:
    code_files: FileLimit
    model_files: FileLimit
    notebook_file: FileLimit
    prediction_file: FileLimit
    submissions_per_day: CountLimit


@dataclass_json(undefined=Undefined.EXCLUDE, letter_case=LetterCase.CAMEL)  # type: ignore[call-overload]
@dataclass
class ProjectComputeQuota:
    available: int
    remaining: int
    used: int
    running: bool


class Project(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["ProjectCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def resource_identifier(self) -> Tuple[int, str]:
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
    def submissions(self):
        from crunch.api._domain.submission import SubmissionCollection

        return SubmissionCollection(
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
        return ProjectSubmitQuota.from_dict(  # type: ignore[attr-defined]
            self._checked_client.api.get_project_submit_quota(
                self.competition.id,
                self.user_id,
                self.name,
            )
        )

    @property
    def compute_quota(self) -> ProjectComputeQuota:
        return ProjectComputeQuota.from_dict(  # type: ignore[attr-defined]
            self._checked_client.api.get_project_compute_quota(
                self.competition.id,
                self.user_id,
                self.name,
            )
        )

    def clone(
        self,
        submission_number: Optional[int],
        include_model: Optional[bool],
    ) -> Dict[str, str]:
        return self._checked_client.api.clone_project(
            self.competition.id,
            self.user_id,
            self.name,
            submission_number,
            include_model,
        )  # pyright: ignore[reportReturnType]


class ProjectCollection(Collection[Project]):

    model = Project

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
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
        user_identifier: Union[int, str] = "@me",
    ) -> List[Project]:
        return self.prepare_models(
            self._checked_client.api.list_projects(
                self.competition.id,
                user_identifier
            ),
            self.competition
        )

    def prepare_model(self, attrs: Union["JsonValue", Project], *args: Any):
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
        competition: Optional["Competition"],
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["ProjectTokenCollection"] = None,
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
    def plain(self) -> Optional[str]:
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
        competition: "Competition",
        client: Optional["Client"] = None
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
        submission_number: Optional[int],
        include_model: Optional[bool],
    ):
        params: Dict[str, Any] = {}

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
