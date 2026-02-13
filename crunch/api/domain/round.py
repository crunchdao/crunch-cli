from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from crunch.api.resource import Collection, Model

if TYPE_CHECKING:
    from crunch.api.client import Client
    from crunch.api.domain.competition import Competition
    from crunch.api.identifiers import RoundIdentifierType


class Round(Model):

    resource_identifier_attribute = "number"

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional[Dict[str, Any]] = None,
        client: Optional["Client"] = None,
        collection: Optional["RoundCollection"] = None
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
    def start(self) -> datetime:
        return datetime.fromisoformat(self._attrs["start"])

    @property
    def end(self) -> datetime:
        return datetime.fromisoformat(self._attrs["end"])

    @property
    def phases(self):
        from crunch.api.domain.phase import PhaseCollection

        return PhaseCollection(
            round=self,
            client=self._client
        )


class RoundCollection(Collection[Round]):

    model = Round

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None,
    ):
        super().__init__(client)

        self.competition = competition

    def get(
        self,
        identifier: "RoundIdentifierType",
    ) -> Round:
        return self.prepare_model(
            self._client.api.get_round(
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
    ) -> List[Round]:
        return self.prepare_models(
            self._client.api.list_rounds(
                self.competition.resource_identifier,
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
        round_identifier
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/rounds/{round_identifier}"
            ),
            json=True
        )
