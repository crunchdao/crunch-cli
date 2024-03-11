import typing
import enum

from ..identifiers import CompetitionIdentifierType
from ..resource import Collection, Model


class Competition(Model):

    resource_identifier_attribute = "name"

    @property
    def name(self):
        return self._attrs["name"]

    @property
    def project(self):
        return self.projects.get_me()

    @property
    def checks(self):
        from .check import CheckCollection

        return CheckCollection(
            competition=self,
            client=self._client
        )

    @property
    def data_releases(self):
        from .data_release import DataReleaseCollection

        return DataReleaseCollection(
            competition=self,
            client=self._client
        )

    @property
    def metrics(self):
        from .metric import MetricCollection

        return MetricCollection(
            competition=self,
            client=self._client
        )

    @property
    def projects(self):
        from .project import ProjectCollection

        return ProjectCollection(
            competition=self,
            client=self._client
        )

    @property
    def quickstarters(self):
        from .quickstarter import QuickstarterCollection

        return QuickstarterCollection(
            competition=self,
            client=self._client
        )

    @property
    def rounds(self):
        from .round import RoundCollection

        return RoundCollection(
            competition=self,
            client=self._client
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
            self._client.api.get_competition(
                id_or_name
            )
        )

    def list(
        self
    ) -> typing.List[Competition]:
        return self.prepare_models(
            self._client.api.list_competitions()
        )


class CompetitionEndpointMixin:

    def list_competitions(
        self
    ):
        return self._result(
            self.get(
                "/v1/competitions"
            ),
            json=True
        )

    def get_competition(
        self,
        identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{identifier}"
            ),
            json=True
        )
