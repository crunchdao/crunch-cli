import typing

from . import api


def to_column_name(metric: api.Metric, column_name: str):
    return f"{column_name}$$meta$${metric.name}"


def filter_metrics(
    metrics: typing.List[api.Metric],
    target_name: typing.Optional[str],
    scorer_function: api.ScorerFunction
):
    filtered = (
        metric
        for metric in metrics
        if metric.scorer_function == scorer_function
    )

    if target_name is not None:
        filtered = (
            metric
            for metric in filtered
            if metric.target.name == target_name
        )

    return list(filtered)
