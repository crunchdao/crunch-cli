from dataclasses import dataclass
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._domain.phase import Phase
    from crunch.api._identifiers import CrunchIdentifierType


@dataclass
class Crunch(ApiModel):
    id: int
    number: int
    published: bool

    phase = Parent["Phase"]()


class CrunchCollection(ApiCollection[Crunch]):

    model = Crunch

    def __init__(self, phase: "Phase"):
        super().__init__(phase.round.competition.client)

        self.phase = phase

    def _attach(self, crunch: Crunch) -> Crunch:
        crunch.phase = self.phase
        return crunch

    def get(self, /, identifier: "CrunchIdentifierType") -> Crunch:
        round = self.phase.round

        return self.from_dict(self.client.api.get_json([
            "v2", "competitions", round.competition.name, "rounds", round.number, "phases", self.phase.type.name, "crunches", identifier
        ]))

    def get_current(self) -> Crunch:
        return self.get("@current")

    def get_next(self) -> Crunch:
        return self.get("@next")

    def get_published(self) -> Crunch:
        return self.get("@published")

    @property
    def current(self) -> Crunch:
        return self.get_current()

    @property
    def next(self) -> Crunch:
        return self.get_next()

    @property
    def published(self) -> Crunch:
        return self.get_published()

    def list(self) -> list[Crunch]:
        round = self.phase.round

        return self.from_dict_array(self.client.api.get_json([
            "v2", "competitions", round.competition.name, "rounds", round.number, "phases", self.phase.type.name, "crunches"
        ]))
