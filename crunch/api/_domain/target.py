from typing import TYPE_CHECKING, Any, List, Optional, Union

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._identifiers import CompetitionIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class Target(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["TargetCollection"] = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition

    @property
    def id(self) -> int:
        return self._attrs["id"]

    @property
    def resource_identifier(self) -> str:
        return self.name

    @property
    def competition(self):
        return self._competition

    @property
    def metrics(self):
        from crunch.api._domain.metric import MetricCollection

        return MetricCollection(
            competition=self._competition,
            target=self,
            client=self._client
        )

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def display_name(self) -> str:
        return self._attrs["displayName"]

    @property
    def virtual(self) -> bool:
        return self._attrs["virtual"]

    @property
    def primary(self) -> bool:
        return self._attrs["primary"]


class TargetCollection(Collection[Target]):

    model = Target

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.competition = competition

    def get(
        self,
        name: str,
    ) -> Target:
        return self.prepare_model(
            self._checked_client.api.get_target(
                self.competition.id,
                name
            )
        )

    def list(
        self,
        name: Optional[str] = None,
        virtual: Optional[bool] = None,
    ) -> List[Target]:
        return self.prepare_models(
            self._checked_client.api.list_targets(
                self.competition.id,
                name,
                virtual,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Target], *args: Any) -> Target:
        return super().prepare_model(
            attrs,
            self.competition,
            *args
        )


class TargetEndpointMixin(EndpointMixin):

    def get_target(
        self,
        competition_identifier: "CompetitionIdentifierType",
        name: str
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets/{name}"
            ),
            json=True
        )

    def list_targets(
        self,
        competition_identifier: "CompetitionIdentifierType",
        name: Optional[str],
        virtual: Optional[bool],
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets",
                params={
                    "name": name,
                    "virtual": virtual,
                }
            ),
            json=True
        )
