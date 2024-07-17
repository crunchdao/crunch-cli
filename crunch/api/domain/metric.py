import enum
import typing

from ..resource import Collection, Model
from .competition import Competition
from .target import Target


class ScorerFunction(enum.Enum):

    BALANCED_ACCURACY = "BALANCED_ACCURACY"
    DOT_PRODUCT = "DOT_PRODUCT"
    F1 = "F1"
    PRECISION = "PRECISION"
    RANDOM = "RANDOM"
    RECALL = "RECALL"
    SPEARMAN = "SPEARMAN"

    def __repr__(self):
        return self.name


class ReducerFunction(enum.Enum):

    NONE = "NONE"
    MEAN = "MEAN"
    PRODUCT_PLUS_MINUS_1 = "PRODUCT_PLUS_MINUS_1"

    def __repr__(self):
        return self.name


class Metric(Model):

    resource_identifier_attribute = "name"

    def __init__(
        self,
        competition: Competition,
        target: Target = None,
        attrs=None,
        client=None,
        collection=None
    ):
        super().__init__(attrs, client, collection)

        self._competition = competition
        self._target = target or Target(competition, attrs["target"], client)

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


class MetricCollection(Collection):

    model = Metric

    def __init__(
        self,
        competition: Competition,
        target: Target,
        client=None
    ):
        super().__init__(client)

        self.competition = competition
        self.target = target

    def __iter__(self) -> typing.Iterator[Metric]:
        return super().__iter__()

    def get(
        self,
        name: str
    ) -> Metric:
        return self.prepare_model(
            self._client.api.get_metric(
                self.competition.id,
                self.target.name,
                name
            )
        )

    def list(
        self
    ) -> Metric:
        return self.prepare_models(
            self._client.api.list_metrics(
                self.competition.id,
                self.target.name if self.target else None,
            )
        )

    def prepare_model(self, attrs):
        return super().prepare_model(
            attrs,
            self.competition,
            self.target
        )


class MetricEndpointMixin:

    def get_metric(
        self,
        competition_identifier,
        target_name,
        metric_name
    ):
        return self._result(
            self.get(
                f"/v1/competitions/{competition_identifier}/targets/{target_name}/metrics/{metric_name}"
            ),
            json=True
        )

    def list_metrics(
        self,
        competition_identifier,
        target_name,
    ):
        url = (
            f"/v1/competitions/{competition_identifier}/targets/{target_name}/metrics"
            if target_name is not None else
            f"/v1/competitions/{competition_identifier}/metrics"
        )

        return self._result(
            self.get(url),
            json=True
        )
