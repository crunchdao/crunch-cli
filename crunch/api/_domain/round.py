from datetime import datetime
from typing import TYPE_CHECKING, Any

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._identifiers import CompetitionIdentifierType, RoundIdentifierType
    from crunch.api._resource import JsonValue


class Round(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        attrs: dict[str, Any] | None = None,
        client: "Client | None" = None,
        collection: "RoundCollection | None" = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def resource_identifier(self) -> int:
        return self.number

    @property
    def competition(self):
        return self._competition

    @property
    def number(self) -> int:
        return self._attrs["number"]

    @property
    def start(self) -> datetime:
        return datetime.fromisoformat(self._attrs["start"])

    @property
    def end(self) -> datetime:
        return datetime.fromisoformat(self._attrs["end"])

    @property
    def phases(self):
        from crunch.api._domain.phase import PhaseCollection

        return PhaseCollection(
            round=self,
            client=self._client
        )


class RoundCollection(Collection[Round]):

    model = Round

    def __init__(
        self,
        competition: "Competition",
        client: "Client | None" = None,
    ):
        super().__init__(client)

        self.competition = competition

    def get(
        self,
        identifier: "RoundIdentifierType",
    ) -> Round:
        return self.prepare_model(
            self._checked_client.api.get_round(
                self.competition.resource_identifier,
                identifier
            )
        )

    def get_current(self):
        return self.get("@current")

    @property
    def current(self):
        return self.get_current()

    def get_last(self):
        return self.get("@last")

    @property
    def last(self):
        return self.get_last()

    def list(
        self
    ) -> list[Round]:
        return self.prepare_models(
            self._checked_client.api.list_rounds(
                self.competition.resource_identifier,
            )
        )

    def prepare_model(self, attrs: "JsonValue | Round", *args: Any) -> Round:
        return super().prepare_model(
            attrs,
            self.competition,
            *args
        )


class RoundEndpointMixin(EndpointMixin):

    def list_rounds(
        self,
        competition_identifier: "CompetitionIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds"
            ),
            json=True
        )

    def get_round(
        self,
        competition_identifier: "CompetitionIdentifierType",
        round_identifier: "RoundIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds/{round_identifier}"
            ),
            json=True
        )
