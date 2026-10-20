from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from crunch.api._base import ApiCollection, ApiModel, Parent
from crunch.api._domain.metric import Unit

if TYPE_CHECKING:
    from crunch.api._domain.project import ProjectReference


@dataclass
class Prediction(ApiModel):
    id: int
    name: str
    valid: bool | None
    success: bool | None
    managed: bool
    total_size: int
    created_at: datetime
    error: str | None = None
    deterministic: bool | None = None
    primary_mean: float | None = None
    primary_unit: Unit | None = None

    project = Parent["ProjectReference"]()

    @property
    def scores(self):
        from crunch.api._domain.score import ScoreCollection
        return ScoreCollection(self)


class PredictionCollection(ApiCollection[Prediction]):

    model = Prediction

    def __init__(self, project: "ProjectReference"):
        super().__init__(project.client)

        self.project = project

    def _attach(self, prediction: Prediction) -> Prediction:
        prediction.project = self.project
        return prediction

    def get(self, /, id: int) -> Prediction:
        return self.from_dict(self.client.api.get_json([
            "v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "predictions", id
        ]))

    def list(self) -> list[Prediction]:
        return self.from_dict_array(self.client.api.get_json([
            "v3", "competitions", self.project.competition.id, "projects", self.project.user_id, self.project.name, "predictions"
        ]))
