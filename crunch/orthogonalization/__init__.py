import collections
import typing

import pandas
import typing_extensions

from .. import api, runner
from .orthogonalize import process, run_from_runner, run_via_api

__all__ = [
    "run_via_api",
    "run_from_runner",
    "run",
    "process",
]


@typing.overload
def run(
    prediction: pandas.DataFrame,
    as_dataframe: typing_extensions.Literal[False]
) -> typing.List[api.Score]:
    ...


@typing.overload
def run(
    prediction: pandas.DataFrame,
    as_dataframe: typing_extensions.Literal[True]
) -> typing.Optional[pandas.DataFrame]:
    ...


def run(
    prediction: pandas.DataFrame,
    as_dataframe=True
):
    if runner.is_inside:
        f = run_from_runner
    else:
        f = run_via_api

    scores = f(prediction)

    if not as_dataframe:
        return scores

    if not len(scores):
        return None

    rows = collections.defaultdict(lambda: collections.defaultdict(dict))
    for score in scores:
        for detail in score.details:
            rows[score.metric.name][detail.key] = detail.value

    result = pandas.DataFrame(rows)
    result.index.name = "moon"

    return result
