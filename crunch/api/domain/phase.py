import dataclasses
import enum
import typing

import dataclasses_json

from ..identifiers import PhaseIdentifierType
from ..resource import Collection, Model
from .round import Round


class PhaseType(enum.Enum):

    SUBMISSION = "SUBMISSION"
    OUT_OF_SAMPLE = "OUT_OF_SAMPLE"

    def __repr__(self):
        return self.name


class Phase(Model):

    resource_identifier_attribute = "type"

    def __init__(
        self,
        round: Round,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._round = round

    @property
    def round(self):
        return self._round

    @property
    def type(self):
        return PhaseType[self.attrs["type"]]

    @property
    def crunches(self):
        from .crunch import CrunchCollection

        return CrunchCollection(
            phase=self,
            client=self.client
        )


class PhaseCollection(Collection):

    model = Phase

    def __init__(
        self,
        round: Round,
        client=None
    ):
        super().__init__(client)

        self.round = round

    def __iter__(self) -> typing.Iterator[Phase]:
        return super().__iter__()

    def get(
        self,
        identifier: PhaseIdentifierType
    ) -> Phase:
        response = self.client.api.get_phase(
            self.round.competition.resource_identifier,
            self.round.resource_identifier,
            identifier
        )

        return self.prepare_model(
            response,
            self.round
        )

    def get_current(self):
        return self.get("@current")

    def get_submission(self):
        return self.get(PhaseType.SUBMISSION)

    def get_out_of_sample(self):
        return self.get(PhaseType.OUT_OF_SAMPLE)

    def list(
        self
    ) -> typing.List[Round]:
        response = self.client.api.list_phases(
            self.round.competition.id,
            self.round.number
        )

        return [
            self.prepare_model(
                item,
                self.round
            )
            for item in response
        ]


class PhaseEndpointMixin:

    def list_phases(
        self,
        competition_identifier,
        round_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases"
            ),
            json=True
        )

    def get_phase(
        self,
        competition_identifier,
        round_identifier,
        phase_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}"
            ),
            json=True
        )
