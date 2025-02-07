import itertools
import typing

from .. import api


def truncate(
    values: set,
    max_size=10
):
    size = len(values)

    is_bigger = size > max_size
    if is_bigger:
        values = list(values)[:max_size]

    string = ', '.join(map(str, values))

    if is_bigger:
        string += f", (...{size - max_size})"

    return string


def delta_message(
    expected: set,
    predicted: set,
):
    missing = set(expected) - set(predicted)
    extras = set(predicted) - set(expected)

    message = ""
    if len(missing):
        message += f"missing [{truncate(missing)}]"

    if len(extras):
        if len(missing):
            message += " "

        message += f"extras [{truncate(extras)}]"

    return message

def group_metrics_by_target(metrics: typing.List[api.Target]):
    return [
        (target, list(metrics))
        for target, metrics in itertools.groupby(
            sorted(
                metrics,
                key=lambda x: x.target.id
            ),
            lambda x: x.target
        )
    ]
