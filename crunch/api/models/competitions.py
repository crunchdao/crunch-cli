import typing

from .resource import Collection, Model
from ..identifiers import CompetitionIdentifierType


class Competition(Model):

    @property
    def name(self):
        return self.attrs["name"]

    @property
    def project(self):
        return self.projects.get_me()

    @property
    def checks(self):
        from .checks import CheckCollection

        return CheckCollection(
            competition=self,
            client=self.client
        )

    @property
    def data_releases(self):
        from .data_releases import DataReleaseCollection

        return DataReleaseCollection(
            competition=self,
            client=self.client
        )

    @property
    def projects(self):
        from .projects import ProjectCollection

        return ProjectCollection(
            competition=self,
            client=self.client
        )

    @property
    def rounds(self):
        from .rounds import RoundCollection

        return RoundCollection(
            competition=self,
            client=self.client
        )


class CompetitionCollection(Collection):

    model = Competition

    def __iter__(self) -> typing.Iterator[Competition]:
        return super().__iter__()

    def get(
        self,
        id_or_name: CompetitionIdentifierType
    ) -> Competition:
        return self.prepare_model(
            self.client.api.get_competition(id_or_name)
        )

    def list(
        self
    ) -> typing.List[Competition]:
        response = self.client.api.list_competitions()

        return [
            self.prepare_model(item)
            for item in response
        ]
