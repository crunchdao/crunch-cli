from typing import TYPE_CHECKING, Iterator, List, Optional

from crunch.api.resource import Collection, Model

if TYPE_CHECKING:
    from crunch.api.client import Client
    from crunch.api.domain.competition import Competition
    from crunch.api.types import Attrs


class Target(Model):

    resource_identifier_attribute = "name"

    def __init__(
        self,
        competition: "Competition",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["TargetCollection"] = None,
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition

    @property
    def id(self) -> int:
        return self._attrs["id"]

    @property
    def competition(self):
        return self._competition

    @property
    def metrics(self):
        from .metric import MetricCollection

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


class TargetCollection(Collection):

    model = Target

    def __init__(
        self,
        competition: "Competition",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.competition = competition

    def __iter__(self) -> Iterator[Target]:
        return super().__iter__()  # type: ignore

    def get(
        self,
        name: str,
    ) -> Target:
        return self.prepare_model(
            self._client.api.get_target(
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
            self._client.api.list_targets(
                self.competition.id,
                name,
                virtual,
            )
        )

    def prepare_model(self, attrs: "Attrs"):
        return super().prepare_model(
            attrs,
            self.competition,
        )


class TargetEndpointMixin:

    def get_target(
        self,
        competition_identifier,
        name
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets/{name}"
            ),
            json=True
        )

    def list_targets(
        self,
        competition_identifier,
        name,
        virtual,
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
