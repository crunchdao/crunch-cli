from itertools import groupby
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Set

if TYPE_CHECKING:
    from crunch_convert.requirements_txt import NamedRequirement

    from crunch.api import Metric
    from crunch.unstructured._file import File


def truncate(
    values: Set[str],
    max_size: int = 10
) -> str:
    size = len(values)

    is_bigger = size > max_size
    if is_bigger:
        values = list(values)[:max_size]  # type: ignore

    string = ', '.join(map(str, values))

    if is_bigger:
        string += f", (...{size - max_size})"

    return string


def delta_message(
    expected: Iterable[Any],
    predicted: Iterable[Any],
) -> str:
    expected = set(expected)
    predicted = set(predicted)

    missing = expected - predicted
    extras = predicted - expected

    message = ""
    if len(missing):
        message += f"missing [{truncate(missing)}]"

    if len(extras):
        if len(missing):
            message += " "

        message += f"extras [{truncate(extras)}]"

    return message


def group_metrics_by_target(metrics: List["Metric"]):
    return [
        (target, list(metrics))
        for target, metrics in groupby(
            sorted(
                metrics,
                key=lambda x: x.target.id
            ),
            lambda x: x.target
        )
    ]


def find_requirement_by_name(
    requirements: List["NamedRequirement"],
    name: str,
) -> Optional["NamedRequirement"]:
    return next((
        requirement
        for requirement in requirements
        if requirement.name.casefold() == name.casefold()
    ), None)


def find_file_by_path(
    files: List["File"],
    path: str,
) -> Optional["File"]:
    return next((
        file
        for file in files
        if file.path.casefold() == path.casefold()
    ), None)
