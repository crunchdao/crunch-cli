import typing

from ..identifiers import RoundIdentifierType
from ..resource import Collection, Model
from .competition import Competition


class Round(Model):

    resource_identifier_attribute = "number"

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
    def number(self) -> int:
        return self._attrs["number"]

    @property
    def phases(self):
        from .phase import PhaseCollection

        return PhaseCollection(
            round=self,
            client=self._client
        )


class RoundCollection(Collection):

    model = Round

    def __init__(
        self,
        competition: Competition,
        client=None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> typing.Iterator[Round]:
        return super().__iter__()

    def get(
        self,
        identifier: RoundIdentifierType
    ) -> Round:
        return self.prepare_model(
            self._client.api.get_round(
                self.competition.id,
                identifier
            )
        )

    def get_current(self):
        return self.get("@current")

    def get_last(self):
        return self.get("@last")

    def list(
        self
    ) -> typing.List[Round]:
        return self.prepare_models(
            self._client.api.list_rounds(
                self.competition.id,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition
        )


class RoundEndpointMixin:

    def list_rounds(
        self,
        competition_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds"
            ),
            json=True
        )

    def get_round(
        self,
        competition_identifier,
        identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds/{identifier}"
            ),
            json=True
        )