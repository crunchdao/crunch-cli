import datetime
import enum
import typing

from ..resource import Collection, Model
from .competition import Competition
from .user import User


class Project(Model):

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
    def user(self) -> User:
        user_id = self._attrs["userId"]

        return self._client.users.get(user_id)

    @property
    def predictions(self):
        from .prediction import PredictionCollection

        return PredictionCollection(
            project=self,
            client=self._client
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
        user_id_or_login: typing.Union[int, str]
    ) -> Project:
        response = self._client.api.get_project(
            self.competition.id,
            user_id_or_login
        )

        return self.prepare_model(
            response,
            self.competition
        )

    def get_me(
        self
    ) -> Project:
        return self.get("@me")

    # TODO Introduce an endpoint instead
    def list(
        self,
    ) -> Project:
        from ..errors import ProjectNotFoundException

        try:
            project = self.get_me()

            return [project]
        except ProjectNotFoundException:
            return []


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
    def project(self):
        project_attrs = self._attrs["project"]

        competition_id = project_attrs["competitionId"]
        competition = self._client.competitions.get(competition_id)

        return competition.projects.prepare_model(
            project_attrs,
            competition
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
        user_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}"
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
