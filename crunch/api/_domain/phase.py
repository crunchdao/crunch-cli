from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiEnum, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._domain.data_release import DataRelease, SizeVariant
    from crunch.api._domain.round import Round
    from crunch.api._identifiers import PhaseIdentifierType


class PhaseType(ApiEnum):

    SUBMISSION = "SUBMISSION"
    OUT_OF_SAMPLE = "OUT_OF_SAMPLE"

    def slug(self):
        if self == PhaseType.SUBMISSION:
            return "submission"

        if self == PhaseType.OUT_OF_SAMPLE:
            return "out-of-sample"

        return self._default_slug()

    def pretty(self):
        if self == PhaseType.SUBMISSION:
            return "Submission"

        if self == PhaseType.OUT_OF_SAMPLE:
            return "Out-of-Sample"

        return self._default_slug()

    def first_chain_height(self):
        if self == PhaseType.OUT_OF_SAMPLE:
            return 2

        return 1

    def _default_slug(self):
        return self.name.lower().replace("_", "-")


@dataclass
class Phase(ApiModel):
    id: int
    type: PhaseType
    start: datetime
    end: datetime

    round = Parent["Round"]()

    @property
    def crunches(self):
        from crunch.api._domain.crunch import CrunchCollection
        return CrunchCollection(self)

    def get_data_release(
        self,
        size_variant: "SizeVariant | None" = None,
    ) -> "DataRelease":
        if self.type != PhaseType.SUBMISSION:
            raise ValueError(f"only the submission phase has a data release, not the {self.type.pretty()} phase")

        competition = self.round.competition

        return competition.data_releases.from_dict(self.round.competition.client.api.get_json(
            ["v2", "competitions", competition.name, "rounds", self.round.number, "phases", "submission", "data-release"],
            params={
                "sizeVariant": size_variant.name if size_variant is not None else None,
            },
        ))


class PhaseCollection(ApiCollection[Phase]):

    model = Phase

    def __init__(self, round: "Round"):
        super().__init__(round.competition.client)

        self.round = round

    def _attach(self, phase: Phase) -> Phase:
        phase.round = self.round
        return phase

    def get(self, /, identifier: "PhaseIdentifierType") -> Phase:
        if isinstance(identifier, PhaseType):
            identifier_string = identifier.name
        else:
            identifier_string = identifier

        return self.from_dict(self.client.api.get_json([
            "v2", "competitions", self.round.competition.name, "rounds", self.round.number, "phases", identifier_string
        ]))

    def get_current(self) -> Phase:
        return self.get("@current")

    def get_submission(self) -> Phase:
        return self.get(PhaseType.SUBMISSION)

    def get_out_of_sample(self) -> Phase:
        return self.get(PhaseType.OUT_OF_SAMPLE)

    @property
    def current(self) -> Phase:
        return self.get_current()

    @property
    def submission(self) -> Phase:
        return self.get_submission()

    @property
    def out_of_sample(self) -> Phase:
        return self.get_out_of_sample()

    def list(self) -> list[Phase]:
        return self.from_dict_array(self.client.api.get_json([
            "v2", "competitions", self.round.competition.id, "rounds", self.round.number, "phases"
        ]))
