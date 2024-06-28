import datetime
import enum
import typing

from ..resource import Collection, Model
from .competition import Competition
from .user import User


class Project(Model):

    resource_identifier_attribute = ("userId", "name")

    def __init__(
        self,
        competition: Competition,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

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
    def user(self) -> User:
        return self._client.users.get(self.user_id)

    @property
    def submissions(self):
        from .submission import SubmissionCollection

        return SubmissionCollection(
            project=self,
            client=self._client
        )

    @property
    def runs(self):
        from .run import RunCollection

        return RunCollection(
            project=self,
            client=self._client
        )

    @property
    def predictions(self):
        from .prediction import PredictionCollection

        return PredictionCollection(
            project=self,
            client=self._client
        )

    def clone(
        self,
        submission_number: typing.Optional[int],
        include_model: typing.Optional[bool],
    ) -> typing.Dict[str, str]:
        return self._client.api.clone_project(
            self.competition.id,
            self.user_id,
            self.name,
            submission_number,
            include_model,
        )


class ProjectCollection(Collection):

    model = Project

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> typing.Iterator[Project]:
        return super().__iter__()

    def get(
        self,
        user_identifier: typing.Union[int, str] = "@me",
        project_identifier: typing.Union[int, str, typing.Literal["@first"]] = "@first"
    ) -> Project:
        return self.prepare_model(
            self._client.api.get_project(
                self.competition.id,
                user_identifier,
                project_identifier
            )
        )

    def list(
        self,
        user_identifier: typing.Union[int, str] = "@me",
    ) -> typing.List[Project]:
        return self.prepare_models(
            self._client.api.list_projects(
                self.competition.id,
                user_identifier
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition
        )


class ProjectTokenType(enum.Enum):

    TEMPORARY = "TEMPORARY"
    PERMANENT = "PERMANENT"

    def __repr__(self):
        return self.name


class ProjectToken(Model):

    def __init__(
        self,
        competition: typing.Optional[Competition],
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def project(self) -> Project:
        project_attrs = self._attrs["project"]

        competition_id = project_attrs["competitionId"]
        competition = self._client.competitions.get(competition_id)

        return competition.projects.prepare_model(
            project_attrs
        )

    @property
    def plain(self) -> str:
        return self._attrs.get("plain")

    @property
    def type(self):
        return ProjectTokenType[self._attrs["type"]]

    @property
    def valid_until(self):
        value = self._attrs.get("validUntil")
        if value is None:
            return None

        return datetime.datetime.fromisoformat(value)


class ProjectTokenCollection(Collection):

    model = ProjectToken

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def upgrade(
        self,
        clone_token: str
    ) -> ProjectToken:
        response = self._client.api.upgrade_project_token(
            clone_token
        )

        return self.prepare_model(
            response,
            self.competition
        )


class ProjectEndpointMixin:

    def get_project(
        self,
        competition_identifier,
        user_identifier,
        project_identifier
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}"
            ),
            json=True
        )

    def list_projects(
        self,
        competition_identifier,
        user_identifier
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}"
            ),
            json=True
        )

    def upgrade_project_token(
        self,
        clone_token
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
        competition_identifier,
        user_identifier,
        project_identifier,
        submission_number,
        include_model,
    ):
        params = {}

        if submission_number is not None:
            params["submissionNumber"] = submission_number

        if include_model is not None:
            params["includeModel"] = include_model

        return self._result(
            self.get(
                f"/v4/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/clone",
                params=params
            ),
            json=True
        )
