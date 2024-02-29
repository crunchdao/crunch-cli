import dataclasses
import typing

import dataclasses_json

from .predictions import Prediction
from .resource import Collection, Model


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
        response = self.client.api.list_scores(
            self.prediction.project.competition.id,
            self.prediction.project.user_id,
            self.prediction.id
        )

        return [
            self.prepare_model(
                item,
                self.prediction
            )
            for item in response
        ]
