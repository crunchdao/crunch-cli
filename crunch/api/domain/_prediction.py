import dataclasses
import enum
import typing

import dataclasses_json


class PredictionTag(enum.Enum):

    USER_RUN_OUTPUT = "USER_RUN_OUTPUT"
    MANAGED_RUN_OUTPUT = "MANAGED_RUN_OUTPUT"
    USER_ORTHOGONALIZE = "USER_ORTHOGONALIZE"


@dataclasses_json.dataclass_json(
    letter_case=dataclasses_json.LetterCase.CAMEL,
    undefined=dataclasses_json.Undefined.EXCLUDE
)
@dataclasses.dataclass(frozen=True)
class Prediction:

    id: int
    name: typing.Optional[str]
    success: typing.Optional[bool]
    error: typing.Optional[str]
    mean: typing.Optional[float]
    tag: PredictionTag
    orthogonalized: bool
    created_at: bool
