from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.data_release import SizeVariant
    from crunch.api._domain.round import Round
    from crunch.api._identifiers import CompetitionIdentifierType, PhaseIdentifierType, RoundIdentifierType
    from crunch.api._resource import JsonValue


class PhaseType(Enum):

    SUBMISSION = "SUBMISSION"
    OUT_OF_SAMPLE = "OUT_OF_SAMPLE"

    def __repr__(self):
        return self.name

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
        return str(self).lower().replace("_", "-")


class Phase(Model[int]):

    def __init__(
        self,
        round: "Round",
        attrs: Optional[Dict[str, Any]] = None,
        client: Optional["Client"] = None,
        collection: Optional["PhaseCollection"] = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._round = round

    @property
    def resource_identifier(self) -> str:
        return self._attrs["type"]

    @property
    def round(self):
        return self._round

    @property
    def start(self):
        return datetime.fromisoformat(self._attrs["start"])

    @property
    def end(self):
        return datetime.fromisoformat(self._attrs["end"])

    @property
    def type(self):
        return PhaseType[self._attrs["type"]]

    @property
    def crunches(self):
        from crunch.api._domain.crunch import CrunchCollection

        return CrunchCollection(
            phase=self,
            client=self._client
        )

    def get_data_release(
        self,
        size_variant: Optional["SizeVariant"] = None,
    ):
        from crunch.api._domain.data_release import DataReleaseCollection

        client = self._client
        assert client is not None

        attrs = client.api.get_submission_phase_data_release(
            self.round.competition.resource_identifier,
            self.round.resource_identifier,
            size_variant,
        )

        competition = self.round.competition
        return DataReleaseCollection(
            competition,
            self._client,
        ).prepare_model(attrs)


class PhaseCollection(Collection[Phase]):

    model = Phase

    def __init__(
        self,
        round: "Round",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.round = round

    def get(
        self,
        identifier: "PhaseIdentifierType"
    ) -> Phase:
        if isinstance(identifier, PhaseType):
            identifier = identifier.name

        return self.prepare_model(
            self._checked_client.api.get_phase(
                self.round.competition.resource_identifier,
                self.round.resource_identifier,
                identifier
            )
        )

    def get_current(self):
        return self.get("@current")

    @property
    def current(self):
        return self.get_current()

    def get_submission(self):
        return self.get(PhaseType.SUBMISSION)

    @property
    def submission(self):
        return self.get_submission()

    def get_out_of_sample(self):
        return self.get(PhaseType.OUT_OF_SAMPLE)

    @property
    def out_of_sample(self):
        return self.get_out_of_sample()

    def list(
        self
    ) -> List[Phase]:
        return self.prepare_models(
            self._checked_client.api.list_phases(
                self.round.competition.id,
                self.round.number
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Phase], *args: Any) -> Phase:
        return super().prepare_model(
            attrs,
            self.round,
            *args
        )


class PhaseEndpointMixin(EndpointMixin):

    def list_phases(
        self,
        competition_identifier: "CompetitionIdentifierType",
        round_identifier: "RoundIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases"
            ),
            json=True
        )

    def get_phase(
        self,
        competition_identifier: "CompetitionIdentifierType",
        round_identifier: "RoundIdentifierType",
        phase_identifier: "PhaseIdentifierType",
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}"
            ),
            json=True
        )

    def get_submission_phase_data_release(
        self,
        competition_identifier: "CompetitionIdentifierType",
        round_identifier: "RoundIdentifierType",
        size_variant: Optional["SizeVariant"] = None,
    ):
        params: Dict[str, Any] = {}
        if size_variant:
            params["sizeVariant"] = size_variant.name

        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/submission/data-release",
                params=params
            ),
            json=True
        )
