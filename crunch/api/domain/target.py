import typing

from ..resource import Collection, Model
from .competition import Competition


class Target(Model):

    resource_identifier_attribute = "name"

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
    def metrics(self):
        from .metric import MetricCollection

        return MetricCollection(
            competition=self._competition,
            target=self,
            client=self._client
        )

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def display_name(self) -> str:
        return self._attrs["displayName"]


class TargetCollection(Collection):

    model = Target

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> typing.Iterator[Target]:
        return super().__iter__()

    def get(
        self,
        name: str
    ) -> Target:
        return self.prepare_model(
            self._client.api.get_target(
                self.competition.id,
                name
            )
        )

    def list(
        self
    ) -> Target:
        return self.prepare_models(
            self._client.api.list_targets(
                self.competition.id
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition
        )


class TargetEndpointMixin:

    def get_target(
        self,
        competition_identifier,
        name
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets/{name}"
            ),
            json=True
        )

    def list_targets(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets"
            ),
            json=True
        )
