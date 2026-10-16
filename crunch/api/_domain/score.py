from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from dataclasses_json import LetterCase, Undefined, dataclass_json

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.prediction import Prediction
    from crunch.api._identifiers import CompetitionIdentifierType, ProjectIdentifierType, UserIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


@dataclass_json(letter_case=LetterCase.CAMEL, undefined=Undefined.EXCLUDE)  # type: ignore[call-overload]
@dataclass(frozen=True)
class ScoreDetail:

    key: Union[str, int]
    value: Optional[float]

    @staticmethod
    def from_dict_array(
        input: List[Dict[str, Any]]
    ) -> List["ScoreDetail"]:
        return [
            ScoreDetail.from_dict(x)  # type: ignore[attr-defined]
            for x in input
        ]


class Score(Model[int]):

    def __init__(
        self,
        prediction: "Prediction",
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["ScoreCollection"] = None,
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
    def details(self) -> List[ScoreDetail]:
        return ScoreDetail.from_dict_array(self._attrs.get("details") or [])


class ScoreCollection(Collection[Score]):

    model = Score

    def __init__(
        self,
        prediction: "Prediction",
        client: Optional["Client"] = None
    ):
        super().__init__(client)

        self.prediction = prediction

    def list(
        self
    ) -> List[Score]:
        return self.prepare_models(
            self._checked_client.api.list_scores(
                self.prediction.project.competition.id,
                self.prediction.project.user_id,
                self.prediction.project.name,
                self.prediction.id
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Score], *args: Any) -> Score:
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
