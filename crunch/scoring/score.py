import collections
import dataclasses
import logging
import typing

import pandas

from .. import api
from . import reducers, scorers


@dataclasses.dataclass
class ScoredMetric:
    value: typing.Optional[float]
    details: typing.Dict[int, float]


def _call_scorer(
    scorer: typing.Callable[[pandas.DataFrame, str, str], float],
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
) -> typing.OrderedDict[int, float]:
    moon_and_id = [column_names.moon, column_names.id]

    y_test = y_test.sort_values(by=moon_and_id)
    merged_df = pandas.merge(y_test, prediction, on=moon_and_id)

    target_column_name = column_names.target
    prediction_column_name = column_names.prediction

    if target_column_name == prediction_column_name:
        target_column_name += "_x"
        prediction_column_name += "_y"

    correlation = merged_df\
        .groupby(column_names.moon, group_keys=False)\
        .apply(lambda group: scorer(
            group,
            target_column_name,
            prediction_column_name,
        ))

    return collections.OrderedDict(correlation.items())


def score(
    logger: logging.Logger,
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    metrics: typing.List[api.Metric],
    y_test_keys: typing.Collection[typing.Union[int, str]],
) -> typing.Dict[str, ScoredMetric]:
    prediction = prediction[[
        column_names.moon,
        column_names.id,
        column_names.prediction
    ]]

    y_test = y_test[[
        column_names.moon,
        column_names.id,
        column_names.target
    ]]

    scores = {}

    for metric in metrics:
        scorer = scorers.REGISTRY.get(metric.scorer_function)
        if scorer is None:
            logger.warn(f"unknown metric - name={metric.name} function={metric.scorer_function.name}")
            continue

        all_details = _call_scorer(
            scorer,
            y_test,
            prediction,
            column_names,
        )

        details = {}
        for moon, value in all_details.items():
            popped = True

            if moon in y_test_keys:
                details[moon] = value
                popped = False

            logger.info(f"score - metric={metric.name} function={metric.scorer_function.name} moon={moon} value={value} popped={popped}")

        values = list(details.values())
        values_count = len(values)

        if values_count == 0:
            reducer_method = "none"
            value = None
        elif values_count == 1:
            reducer_method = "first"
            value = values[0]
        else:
            reducer_method = metric.reducer_function.name
            reducer = reducers.REGISTRY[metric.reducer_function]
            value = reducer(values)

        logger.info(f"score - metric={metric.name} scorer={metric.scorer_function.name} reducer={reducer_method} value={value}")

        scores[metric.name] = ScoredMetric(
            value=value,
            details=all_details,
        )

    return scores
