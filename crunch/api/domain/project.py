import enum
import typing

from ..resource import Collection, Model
from .competition import Competition


class ProjectTokenType(enum.Enum):

    TEMPORARY = "TEMPORARY"
    PERMANENT = "PERMANENT"

    def __repr__(self):
        return self.name


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
    def user_id(self):
        return self.attrs["userId"]

    @property
    def predictions(self):
        from .prediction import PredictionCollection

        return PredictionCollection(
            project=self,
            client=self.client
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
        response = self.client.api.get_project(
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
