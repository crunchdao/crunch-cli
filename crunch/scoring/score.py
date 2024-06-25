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
    details: typing.List["ScoredMetricDetail"]


@dataclasses.dataclass
class ScoredMetricDetail:
    key: typing.Union[str, int]
    value: float


def _call_scorer(
    scorer: typing.Callable[[pandas.DataFrame, str, str], float],
    merged: pandas.DataFrame,
    column_names: api.ColumnNames,
    target_column_names: api.TargetColumnNames,
) -> typing.OrderedDict[int, float]:
    target_column_name = target_column_names.input
    prediction_column_name = target_column_names.output

    if target_column_name == prediction_column_name:
        target_column_name += "_x"
        prediction_column_name += "_y"

    correlation = merged\
        .groupby(column_names.moon, group_keys=False)\
        .apply(lambda group: scorer(
            group,
            target_column_name,
            prediction_column_name,
        ))

    return collections.OrderedDict(correlation.items())


def score(
    competition_format: api.CompetitionFormat,
    logger: logging.Logger,
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    metrics: typing.List[api.Metric],
    y_test_keys: typing.Collection[typing.Union[int, str]]
) -> typing.Dict[int, ScoredMetric]:

    if competition_format == api.CompetitionFormat.TIMESERIES:
        from ._format.timeseries import merge

        merged = merge(
            y_test,
            prediction,
            column_names
        )

    elif competition_format == api.CompetitionFormat.DAG:
        from ._format.dag import merge

        merged = merge(
            y_test,
            prediction,
            column_names,
        )

    else:
        raise ValueError(f"unsupported competition format: {competition_format}")

    # print(merged)

    scores = {}

    for metric in metrics:
        target_name = metric.target.name

        target_column_names = column_names.get_target_by_name(target_name)
        if target_column_names is None:
            logger.warn(f"unknown target column names - target_name={target_name} known_target_names={list(column_names.target_names)}")
            continue

        scorer = scorers.REGISTRY.get(metric.scorer_function)
        if scorer is None:
            logger.warn(f"unknown metric - target_name={target_name} metric_name={metric.name} function={metric.scorer_function.name}")
            continue

        all_details = _call_scorer(
            scorer,
            merged,
            column_names,
            target_column_names
        )

        details = {}
        for moon, value in all_details.items():
            popped = True

            if moon in y_test_keys:
                details[moon] = value

                popped = False

            logger.info(f"score - target_name={target_name} metric_name={metric.name} function={metric.scorer_function.name} moon={moon} value={value} popped={popped}")

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

        scores[metric.id] = ScoredMetric(
            value=value,
            details=[
                ScoredMetricDetail(key, value)
                for key, value in all_details.items()
            ],
        )

    return scores
