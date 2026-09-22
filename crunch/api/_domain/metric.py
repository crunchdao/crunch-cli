from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional, Union

from dataclasses_json import Undefined, dataclass_json

from crunch.api._resource import Collection, EndpointMixin, Model

if TYPE_CHECKING:
    from crunch.api._client import Client
    from crunch.api._domain.competition import Competition
    from crunch.api._domain.target import Target
    from crunch.api._identifiers import CompetitionIdentifierType
    from crunch.api._resource import JsonValue
    from crunch.api._types import Attrs


class ScorerFunction(Enum):

    BALANCED_ACCURACY = "BALANCED_ACCURACY"
    DOT_PRODUCT = "DOT_PRODUCT"
    F1 = "F1"
    PRECISION = "PRECISION"
    RANDOM = "RANDOM"
    RECALL = "RECALL"
    SPEARMAN = "SPEARMAN"

    META__EXECUTION_TIME = "META__EXECUTION_TIME"

    CUSTOM__MID_ONE__PROFIT_AND_LOSS_WITH_TRANSACTION_COST = "CUSTOM__MID_ONE__PROFIT_AND_LOSS_WITH_TRANSACTION_COST"
    CUSTOM__BROAD__SCORING = "CUSTOM__BROAD__SCORING"

    @property
    def is_meta(self):
        return self.name.startswith("META__")

    def __repr__(self):
        return self.name


class ReducerFunction(Enum):

    NONE = "NONE"
    SUM = "SUM"
    MEAN = "MEAN"
    PRODUCT_PLUS_MINUS_1 = "PRODUCT_PLUS_MINUS_1"

    def __repr__(self):
        return self.name


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass(frozen=True)
class Unit:

    prefix: Optional[str]
    scale: int
    suffix: Optional[str]


class Metric(Model[int]):

    def __init__(
        self,
        competition: "Competition",
        target: Optional["Target"] = None,
        attrs: Optional["Attrs"] = None,
        client: Optional["Client"] = None,
        collection: Optional["MetricCollection"] = None,
    ):
        from crunch.api._domain.target import Target

        super().__init__(attrs=attrs, client=client, collection=collection)

        self._competition = competition
        self._target = target or Target(competition, (attrs or {})["target"], client)

    @property
    def resource_identifier(self) -> str:
        return self.name

    @property
    def competition(self):
        return self._competition

    @property
    def target(self):
        return self._target

    @property
    def name(self) -> str:
        return self._attrs["name"]

    @property
    def display_name(self) -> str:
        return self._attrs["displayName"]

    @property
    def weight(self) -> int:
        return self._attrs["weight"]

    @property
    def score(self) -> bool:
        return self._attrs["score"]

    @property
    def multiplier(self) -> float:
        return self._attrs["multiplier"]

    @property
    def scorer_function(self):
        return ScorerFunction[self._attrs["scorerFunction"]]

    @property
    def reducer_function(self):
        return ReducerFunction[self._attrs["reducerFunction"]]

    @property
    def unit(self) -> Unit:
        return Unit.from_dict(self._attrs["unit"])  # type: ignore[attr-defined]


class MetricCollection(Collection[Metric]):

    model = Metric

    def __init__(
        self,
        competition: "Competition",
        target: "Target",
        client: Optional["Client"] = None,
    ):
        super().__init__(client)

        self.competition = competition
        self.target = target


    def get(
        self,
        name: str
    ) -> Metric:
        return self.prepare_model(
            self._checked_client.api.get_metric(
                self.competition.id,
                self.target.name,
                name
            )
        )

    def list(
        self
    ) -> List[Metric]:
        return self.prepare_models(
            self._checked_client.api.list_metrics(
                self.competition.id,
                self.target.name if self.target else None,
            )
        )

    def prepare_model(self, attrs: Union["JsonValue", Metric], *args: Any) -> Metric:
        return super().prepare_model(
            attrs,
            self.competition,
            self.target,
            *args
        )


class MetricEndpointMixin(EndpointMixin):

    def get_metric(
        self,
        competition_identifier: "CompetitionIdentifierType",
        target_name: Optional[str],
        metric_name: str
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets/{target_name}/metrics/{metric_name}"
            ),
            json=True
        )

    def list_metrics(
        self,
        competition_identifier: "CompetitionIdentifierType",
        target_name: Optional[str],
    ) -> "JsonValue":
        url = (
            f"/v1/competitions/{competition_identifier}/targets/{target_name}/metrics"
            if target_name is not None else
            f"/v1/competitions/{competition_identifier}/metrics"
        )

        return self._result(
            self.get(url),
            json=True
        )
