from typing import TYPE_CHECKING, Any, List, Optional, Union

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.phase import Phase
    from crunch.api._identifiers import CompetitionIdentifierType, CrunchIdentifierType, PhaseIdentifierType, RoundIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class Crunch(Model[int]):

    def __init__(
        self,
        phase: "Phase",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["CrunchCollection"] = None
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._phase = phase

    @property
    def resource_identifier(self) -> int:
        return self.number

    @property
    def phase(self):
        return self._phase

    @property
    def number(self) -> int:
        return self._attrs["number"]

    @property
    def published(self) -> bool:
        return self._attrs["published"]


class CrunchCollection(Collection[Crunch]):

    model = Crunch

    def __init__(
        self,
        phase: "Phase",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.phase = phase


    def get(
        self,
        identifier: "CrunchIdentifierType"
    ) -> Crunch:
        return self.prepare_model(
            self._checked_client.api.get_crunch(
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
    ) -> List[Crunch]:
        return self.prepare_models(
            self._checked_client.api.list_crunches(
                self.phase.round.competition.resource_identifier,
                self.phase.round.resource_identifier,
                self.phase.resource_identifier,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Crunch], *args: Any) -> Crunch:
        return super().prepare_model(
            attrs,
            self.phase,
            *args
        )


class CrunchEndpointMixin(EndpointMixin):

    def list_crunches(
        self,
        competition_identifier: "CompetitionIdentifierType",
        round_identifier: "RoundIdentifierType",
        phase_identifier: "PhaseIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}/crunches"
            ),
            json=True
        )

    def get_crunch(
        self,
        competition_identifier: "CompetitionIdentifierType",
        round_identifier: "RoundIdentifierType",
        phase_identifier: "PhaseIdentifierType",
        crunch_identifier: "CrunchIdentifierType"
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/rounds/{round_identifier}/phases/{phase_identifier}/crunches/{crunch_identifier}"
            ),
            json=True
        )
