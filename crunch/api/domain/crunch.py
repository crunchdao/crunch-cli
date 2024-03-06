import typing

from ..identifiers import CrunchIdentifierType
from ..resource import Collection, Model
from .phase import Phase


class Crunch(Model):

    resource_identifier_attribute = "number"

    def __init__(
        self,
        phase: Phase,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._phase = phase

    @property
    def phase(self):
        return self._phase

    @property
    def number(self) -> int:
        return self._attrs["number"]


class CrunchCollection(Collection):

    model = Crunch

    def __init__(
        self,
        phase: Phase,
        client=None
    ):
        super().__init__(client)

        self.phase = phase

    def __iter__(self) -> typing.Iterator[Crunch]:
        return super().__iter__()

    def get(
        self,
        identifier: CrunchIdentifierType
    ) -> Crunch:
        return self.prepare_model(
            self._client.api.get_crunch(
                self.phase.round.competition.resource_identifier,
                self.phase.round.resource_identifier,
                self.phase.resource_identifier,
                identifier
            )
        )

    def get_current(self):
        return self.get("@current")

    def get_next(self):
        return self.get("@last")

    def get_published(self):
        return self.get("@published")

    def list(
        self
    ) -> typing.List[Crunch]:
        return self.prepare_models(
            self._client.api.list_crunches(
                self.phase.round.competition.resource_identifier,
                self.phase.round.resource_identifier,
                self.phase.resource_identifier,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.phase
        )


class CrunchEndpointMixin:

    def list_crunches(
        self,
        competition_identifier,
        round_identifier,
        phase_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}/crunches"
            ),
            json=True
        )

    def get_crunch(
        self,
        competition_identifier,
        round_identifier,
        phase_identifier,
        crunch_identifier
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}/crunches/{crunch_identifier}"
            ),
            json=True
        )
