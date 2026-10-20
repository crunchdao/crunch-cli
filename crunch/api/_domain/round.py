from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition
    from crunch.api._identifiers import RoundIdentifierType


@dataclass
class Round(ApiModel):
    id: int
    number: int
    start: datetime
    end: datetime

    competition = Parent["Competition"]()

    @property
    def phases(self):
        from crunch.api._domain.phase import PhaseCollection
        return PhaseCollection(self)


class RoundCollection(ApiCollection[Round]):

    model = Round

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, round: Round) -> Round:
        round.competition = self.competition
        return round

    def get(self, /, identifier: "RoundIdentifierType") -> Round:
        return self.from_dict(self.client.api.get_json([
            "v1", "competitions", self.competition.name, "rounds", identifier
        ]))

    def get_current(self) -> Round:
        return self.get("@current")

    def get_last(self) -> Round:
        return self.get("@last")

    @property
    def current(self) -> Round:
        return self.get_current()

    @property
    def last(self) -> Round:
        return self.get_last()

    def list(self) -> list[Round]:
        return self.from_dict_array(self.client.api.get_json([
            "v1", "competitions", self.competition.name, "rounds"
        ]))
