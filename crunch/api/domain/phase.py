import enum
import typing

from ..identifiers import PhaseIdentifierType
from ..resource import Collection, Model
from .round import Round


class PhaseType(enum.Enum):

    SUBMISSION = "SUBMISSION"
    OUT_OF_SAMPLE = "OUT_OF_SAMPLE"

    def __repr__(self):
        return self.name

    def pretty(self):
        if self == PhaseType.SUBMISSION:
            return "Submission"

        if self == PhaseType.OUT_OF_SAMPLE:
            return "Out-of-Sample"

        return str(self).lower().replace("_", "-")


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
        return PhaseType[self._attrs["type"]]

    @property
    def crunches(self):
        from .crunch import CrunchCollection

        return CrunchCollection(
            phase=self,
            client=self._client
        )

    def get_data_release(self):
        from .data_release import DataReleaseCollection, DataRelease

        attrs = self._client.api.get_submission_phase_data_release(
            self.round.competition.resource_identifier,
            self.round.resource_identifier,
        )

        competition = self.round.competition
        data_release = DataReleaseCollection(
            competition,
            self._client
        ).prepare_model(attrs)

        return typing.cast(DataRelease, data_release)


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
        if isinstance(identifier, PhaseType):
            identifier = identifier.name

        return self.prepare_model(
            self._client.api.get_phase(
                self.round.competition.resource_identifier,
                self.round.resource_identifier,
                identifier
            )
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
        return self.prepare_models(
            self._client.api.list_phases(
                self.round.competition.id,
                self.round.number
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.round
        )


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

    def get_submission_phase_data_release(
        self,
        competition_identifier,
        round_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/submission/data-release"
            ),
            json=True
        )
