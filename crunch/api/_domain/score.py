from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from crunch.api._resource import Collection, EndpointMixin, Model
from crunch.api._serialization import JsonDataClassMixin

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.prediction import Prediction
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


@dataclass(frozen=True)
class ScoreDetail(JsonDataClassMixin):
    key: str | int
    value: float | None


class Score(Model[int]):

    def __init__(
        self,
        prediction: "Prediction",
        attrs: "Attrs | None" = None,
        client: "Client | None" = None,
        collection: "ScoreCollection | None" = None,
    ):
        super().__init__(attrs=attrs, client=client, collection=collection)

        self._prediction = prediction

    @property
    def prediction(self):
        return self._prediction

    @property
    def metric(self):
        from crunch.api._domain.metric import Metric

        metric_attrs = self._attrs.get("metric")
        if metric_attrs is not None:
            return Metric(self._prediction.project.competition, None, metric_attrs)

        return None

    @property
    def value(self) -> float:
        return self._attrs["value"]

    @property
    def details(self) -> list[ScoreDetail]:
        return ScoreDetail.from_dict_array(self._attrs.get("details") or [])


class ScoreCollection(Collection[Score]):

    model = Score

    def __init__(
        self,
        prediction: "Prediction",
        client: "Client | None" = None
    ):
        super().__init__(client)

        self.prediction = prediction

    def list(
        self
    ) -> list[Score]:
        return self.prepare_models(
            self._checked_client.api.list_scores(
                self.prediction.project.competition.id,
                self.prediction.project.user_id,
                self.prediction.project.name,
                self.prediction.id
            )
        )

    def prepare_model(self, attrs: "JsonValue | Score", *args: Any) -> Score:
        return super().prepare_model(
            attrs,
            self.prediction,
            *args
        )


class ScoreEndpointMixin(EndpointMixin):

    def list_scores(
        self,
        competition_identifier: "CompetitionIdentifierType",
        user_identifier: "UserIdentifierType",
        project_identifier: "ProjectIdentifierType",
        prediction_id: int
    ):
        return self._result(
            self.get(
                f"/v3/competitions/{competition_identifier}/projects/{user_identifier}/{project_identifier}/predictions/{prediction_id}/scores"
            ),
            json=True
        )
