from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping

from crunch.api._base import ApiCollection, ApiModel, Parent
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._domain.metric import Metric
    from crunch.api._domain.prediction import Prediction


@dataclass(frozen=True)
class ScoreDetail(JsonDataClassMixin):
    key: str | int
    value: float | None


@dataclass
class Score(ApiModel):
    id: int
    success: bool
    created_at: datetime
    value: float | None = None
    details: list[ScoreDetail] = field(default_factory=list[ScoreDetail])

    prediction = Parent["Prediction"]()
    metric = Parent["Metric"]()  # not directly attached, comes from payload


class ScoreCollection(ApiCollection[Score]):

    model = Score

    def __init__(self, prediction: "Prediction"):
        super().__init__(prediction.project.client)

        self.prediction = prediction

    def from_dict(self, payload: Mapping[str, Any]) -> Score:
        score = super().from_dict(payload)
        score.metric = self.prediction.project.competition.metrics.from_dict(payload["metric"])

        return score

    def _attach(self, score: Score) -> Score:
        score.prediction = self.prediction
        return score

    def list(self) -> list[Score]:
        project = self.prediction.project

        return self.from_dict_array(self.client.api.get_json(
            ["v3", "competitions", project.competition.id, "projects", project.user_id, project.name, "predictions", self.prediction.id, "scores"]
        ))
