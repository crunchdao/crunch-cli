import typing

from .competitions import Competition
from .resource import Collection, Model


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
        from .predictions import PredictionCollection

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
