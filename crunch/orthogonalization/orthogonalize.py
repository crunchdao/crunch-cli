import typing

import pandas

from .. import __version__, api, vendor


def run_via_api(
    dataframe: pandas.DataFrame,
    timeout: int,
) -> typing.List[api.Score]:
    _, project = api.Client.from_project()

    competition = project.competition
    round = competition.rounds.current

    return round.orthogonalize(
        dataframe,
        timeout=timeout
    )


def run_from_runner(
    dataframe: pandas.DataFrame,
) -> typing.List[api.Score]:
    from ._runner import delegate
    return delegate(dataframe)


def process(
    competition_name: str,
    prediction: pandas.DataFrame,
    orthogonalization_data: typing.Any,
    column_names: api.ColumnNames,
):
    module = vendor.get(competition_name)

    orthogonalize = getattr(module, "orthogonalize", None)
    if orthogonalize is None:
        raise ValueError(f"no orthogonalize function for {competition_name}")

    dataframe = orthogonalize(
        competition_name=competition_name,
        prediction=prediction,
        orthogonalization_data=orthogonalization_data,
        column_names=column_names
    )

    if not isinstance(dataframe, pandas.DataFrame):
        raise ValueError(f"orthogonalize returned: {dataframe.__class__.__name__}")

    return dataframe
