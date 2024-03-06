import dataclasses
import typing

import dataclasses_json

from ..resource import Collection, Model
from .prediction import Prediction


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class ScoreSummary:

    mean: str
    metrics: typing.Dict[str, typing.Optional[float]]


class Score(Model):

    def __init__(
        self,
        prediction: Prediction,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._prediction = prediction

    @property
    def prediction(self):
        return self._prediction

    @property
    def metric(self):
        from .metric import Metric

        metric_attrs = self._attrs.get("metric")
        if metric_attrs is not None:
            return Metric(None, metric_attrs)
    
        return None

    @property
    def details(self) -> typing.Dict[str, str]:
        return self._attrs.get("details") or {}


class ScoreCollection(Collection):

    model = Score

    def __init__(
        self,
        prediction: Prediction,
        client=None
    ):
        super().__init__(client)

        self.prediction = prediction

    def __iter__(self) -> typing.Iterator[Score]:
        return super().__iter__()

    def list(
        self
    ) -> typing.List[Score]:
        return self.prepare_models(
            self._client.api.list_scores(
                self.prediction.project.competition.id,
                self.prediction.project.user_id,
                self.prediction.id
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.prediction
        )


class ScoreEndpointMixin:

    def list_scores(
        self,
        competition_identifier,
        user_identifier,
        prediction_id
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions/{prediction_id}/scores"
            ),
            json=True
        )

    def get_score_summary(
        self,
        competition_identifier,
        user_identifier,
        prediction_id
    ):
        return self._result(
            self.get(
                f"/v2/competitions/{competition_identifier}/projects/{user_identifier}/predictions/{prediction_id}/scores/summary"
            ),
            json=True
        )
