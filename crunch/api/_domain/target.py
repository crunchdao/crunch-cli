from dataclasses import dataclass
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition


@dataclass
class Target(ApiModel):
    id: int
    name: str
    display_name: str
    virtual: bool
    primary: bool

    competition = Parent["Competition"]()

    @property
    def metrics(self):
        from crunch.api._domain.metric import MetricCollection
        return MetricCollection(self)


class TargetCollection(ApiCollection[Target]):

    model = Target

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def _attach(self, target: Target) -> Target:
        target.competition = self.competition
        return target

    def get(self, /, name: str) -> Target:
        return self.from_dict(self.client.api.get_json([
            "v1", "competitions", self.competition.id, "targets", name
        ]))

    def list(
        self,
        *,
        name: str | None = None,
        virtual: bool | None = None,
    ) -> list[Target]:
        return self.from_dict_array(self.client.api.get_json(
            ["v1", "competitions", self.competition.id, "targets"],
            params={
                "name": name,
                "virtual": virtual,
            },
        ))

    @property
    def primary(self) -> Target | None:
        return next(
            (
                target
                for target in self.list()
                if target.primary
            ),
            None
        )
