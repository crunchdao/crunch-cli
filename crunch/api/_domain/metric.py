from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

from crunch.api._base import ApiCollection, ApiModel, Parent
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.target import Target


@dataclass(frozen=True)
class Unit(JsonDataClassMixin):
    prefix: str | None
    scale: int
    suffix: str | None

    def format_value(self, value: float) -> str:
        prefix = self.prefix or ''
        rounded_value = round(value, self.scale) if self.scale else int(value)
        suffix = self.suffix or ''

        return f"{prefix}{rounded_value}{suffix}"


@dataclass
class Metric(ApiModel):

    id: int
    name: str
    display_name: str
    unit: Unit
    weight: int
    score: bool
    multiplier: float

    target = Parent["Target"]()

    @property
    def competition(self) -> "Competition":
        return self.target.competition


class MetricCollection(ApiCollection[Metric]):

    model = Metric

    def __init__(self, target: "Target"):
        super().__init__(target.competition.client)

        self.target = target

    def _attach(self, metric: Metric) -> Metric:
        metric.target = self.target
        return metric

    def get(self, /, name: str) -> Metric:
        return self.from_dict(self.client.api.get_json(
            ["v1", "competitions", self.target.competition.id, "targets", self.target.name, "metrics", name]
        ))

    def list(self) -> list[Metric]:
        return self.from_dict_array(self.client.api.get_json(
            ["v1", "competitions", self.target.competition.id, "targets", self.target.name, "metrics"]
        ))


class CompetitionMetricCollection(ApiCollection[Metric]):

    model = Metric

    def __init__(self, competition: "Competition"):
        super().__init__(competition.client)

        self.competition = competition

    def from_dict(self, payload: Mapping[str, Any]) -> Metric:
        target = self.competition.targets.from_dict(payload["target"])
        return target.metrics.from_dict(payload)

    def list(self) -> list[Metric]:
        return self.from_dict_array(self.client.api.get_json(
            ["v1", "competitions", self.competition.id, "metrics"]
        ))
