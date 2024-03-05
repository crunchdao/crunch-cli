import typing

import pandas

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
    dataframe: pandas.DataFrame,
    raw: typing.Literal[True]
) -> typing.List[api.Score]:
    ...


@typing.overload
def run(
    dataframe: pandas.DataFrame,
    raw: typing.Literal[False]
) -> pandas.DataFrame:
    ...


def run(
    dataframe: pandas.DataFrame,
    raw=False
):
    if runner.is_inside:
        f = run_from_runner
    else:
        f = run_via_api

    scores = f(dataframe)

    if raw:
        return scores

    result = pandas.json_normalize([
        score._attrs
        for score in scores
    ])

    del result["id"]

    return result
