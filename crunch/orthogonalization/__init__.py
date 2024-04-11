import collections
import typing

import pandas
import requests
import typing_extensions

from .. import api, runner
from .orthogonalize import process, run_from_runner, run_via_api

__all__ = [
    "run_via_api",
    "run_from_runner",
    "run",
    "process",
]

DEFAULT_MAX_RETRY = 3
DEFAULT_TIMEOUT = 60


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
    as_dataframe=True,
    max_retry=DEFAULT_MAX_RETRY,
    timeout=DEFAULT_TIMEOUT,
):
    if runner.is_inside:
        scores = run_from_runner(prediction)
    else:
        max_retry = max(1, max_retry)
        for retry in range(1, max_retry + 1):
            try:
                scores = run_via_api(prediction, timeout)
                break
            except (requests.ConnectionError, requests.Timeout):
                if retry == max_retry:
                    raise

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
