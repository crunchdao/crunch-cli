import dataclasses
import datetime
import enum
import typing

import dataclasses_json

from ._common import datetime_config


class MetricFunction(enum.Enum):

    SPEARMAN = "SPEARMAN"
    F1 = "F1"
    RECALL = "RECALL"
    PRECISION = "PRECISION"
    DOT_PRODUCT = "DOT_PRODUCT"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class Metric:

    id: int
    name: str
    display_name: str
    weight: int
    score: bool
    multiplier: float
    function: MetricFunction
    created_at: datetime.datetime = dataclasses.field(metadata=datetime_config)


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class Score:

    id: int
    success: bool
    metric: Metric
    value: typing.Optional[float]
    details: typing.Optional[typing.Dict[str, typing.Optional[float]]]
    created_at: datetime.datetime = dataclasses.field(metadata=datetime_config)
