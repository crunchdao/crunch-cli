import collections
import dataclasses
import logging
import typing
import warnings

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


def merge_keys(
    column_names: api.ColumnNames,
    target_column_names: api.TargetColumnNames,
):
    input = target_column_names.input or column_names.input
    output = target_column_names.output or column_names.output

    if input == output:
        input += "_x"
        output += "_y"

    return (
        input,
        output,
    )


def _call_scorer_grouped(
    scorer: typing.Callable[[pandas.DataFrame, str, str], float],
    merged: pandas.DataFrame,
    column_names: api.ColumnNames,
    target_column_names: api.TargetColumnNames,
) -> typing.OrderedDict[int, float]:
    (
        target_column_name,
        prediction_column_name
    ) = merge_keys(column_names, target_column_names)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)

        correlation = merged \
            .groupby(column_names.moon, group_keys=False) \
            .apply(lambda group: scorer(
                group,
                target_column_name,
                prediction_column_name,
            ))

    return collections.OrderedDict(correlation.items())


def _call_scorer_full(
    scorer: typing.Callable[[pandas.DataFrame, str, str], float],
    merged: pandas.DataFrame,
    column_names: api.ColumnNames,
    target_column_names: api.TargetColumnNames,
) -> typing.OrderedDict[int, float]:
    (
        target_column_name,
        prediction_column_name
    ) = merge_keys(column_names, target_column_names)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)

        return scorer(
            merged,
            target_column_name,
            prediction_column_name,
        )


def _reduce(
    logger: logging.Logger,
    target_name: str,
    all_details: typing.OrderedDict[str, float],
    y_test_keys: typing.Set[typing.Union[str, int]],
    metric: api.Metric,
    scorer: typing.Callable[[pandas.DataFrame, str, str], float],
    merged: pandas.DataFrame,
    column_names: api.ColumnNames,
    target_column_names: api.TargetColumnNames,
):
    details = {}
    for moon, value in all_details.items():
        popped = True

        if moon in y_test_keys:
            details[moon] = value

            popped = False

        logger.info(f"score - target_name={target_name} metric_name={metric.name} scorer_function={metric.scorer_function.name} moon={moon} value={value} popped={popped}")

    if metric.reducer_function == api.ReducerFunction.NONE:
        reducer_method = api.ReducerFunction.NONE.name

        filtered = merged[merged[column_names.moon].isin(y_test_keys)]

        value = _call_scorer_full(
            scorer,
            filtered,
            column_names,
            target_column_names,
        )
    else:
        values = list(details.values())
        values_count = len(values)

        if values_count == 0:
            reducer_method = "no-value"
            value = None
        elif values_count == 1:
            reducer_method = "first"
            value = values[0]
        else:
            reducer_method = metric.reducer_function.name
            reducer = reducers.REGISTRY[metric.reducer_function]
            value = reducer(values)

    logger.info(f"score - target_name={target_name} metric_name={metric.name} scorer_function={metric.scorer_function.name} reducer={reducer_method} value={value}")

    return value


def score(
    competition_format: api.CompetitionFormat,
    logger: logging.Logger,
    y_test: pandas.DataFrame,
    prediction: pandas.DataFrame,
    column_names: api.ColumnNames,
    metrics: typing.List[api.Metric],
    y_test_keys: typing.Collection[typing.Union[int, str]]
) -> typing.Dict[int, ScoredMetric]:
    logger.warning(f"scoring - prediction.len={len(prediction)}")

    if competition_format == api.CompetitionFormat.TIMESERIES:
        from ._format.timeseries import merge

        merged = merge(
            y_test,
            prediction,
            column_names,
            metrics,
        )

    elif competition_format == api.CompetitionFormat.DAG:
        from ._format.dag import merge

        merged = merge(
            y_test,
            prediction,
            column_names,
        )

    elif competition_format == api.CompetitionFormat.STREAM:
        from ._format.stream import merge

        merged = merge(
            y_test,
            prediction,
            column_names,
        )

    else:
        raise ValueError(f"unsupported competition format: {competition_format}")

    logger.warning(f"merged - merged.len={len(merged)}")

    if not len(merged):
        raise ValueError(f"merged dataframe is empty: {merged}")

    scores = {}

    for metric in metrics:
        target_name = metric.target.name

        target_column_names = column_names.get_target_by_name(target_name)
        if target_column_names is None:
            logger.warning(f"unknown target column names - target_name={target_name} known_target_names={list(column_names.target_names)}")
            continue

        scorer = scorers.REGISTRY.get(metric.scorer_function)
        if scorer is None:
            logger.warning(f"unknown metric - target_name={target_name} metric_name={metric.name} function={metric.scorer_function.name}")
            continue

        all_details = _call_scorer_grouped(
            scorer,
            merged,
            column_names,
            target_column_names
        )

        value = _reduce(
            logger,
            target_name,
            all_details,
            y_test_keys,
            metric,
            scorer,
            merged,
            column_names,
            target_column_names,
        )

        scores[metric.id] = ScoredMetric(
            value=value,
            details=[
                ScoredMetricDetail(key, value)
                for key, value in all_details.items()
            ],
        )

    return scores
